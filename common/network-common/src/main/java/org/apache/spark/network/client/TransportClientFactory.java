/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Factory for creating {@link TransportClient}s by using createClient.
 * 创建 TransportClient 传输客户端的工厂类。
 *
 * The factory maintains a connection pool to other hosts and should return the same
 * TransportClient for the same remote host. It also shares a single worker thread pool for
 * all TransportClients.
 * 工厂维护与其他主机的连接池，并且应为同一远程主机返回相同的 TransportClient。
 * 它还为所有 TransportClient 共享一个工作线程池。
 *
 * TransportClients will be reused whenever possible. Prior to completing the creation of a new
 * TransportClient, all given {@link TransportClientBootstrap}s will be run.
 * TransportClients 将在可能的情况下被重用。
 * 在完成创建新的 TransportClient 之前，将运行所有给定的 TransportClientBootstrap。
 */
public class TransportClientFactory implements Closeable {

  /**
   * A simple data structure to track the pool of clients between two peer nodes.
   * 在两个对等节点间维护的关于 TransportClient 的池子。ClientPool 是 TransportClientFactory 的内部组件。
   */
  private static class ClientPool {
    TransportClient[] clients;
    Object[] locks;

    ClientPool(int size) {
      clients = new TransportClient[size];
      locks = new Object[size];
      for (int i = 0; i < size; i++) {
        locks[i] = new Object();
      }
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(TransportClientFactory.class);

  private final TransportContext context;
  private final TransportConf conf;
  private final List<TransportClientBootstrap> clientBootstraps;
  private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;

  /**
   * Random number generator for picking connections between peers.
   * 随机数生成器，对 Socket 地址对应的连接池 ClientPool 中缓存的 TransportClient 进行随机选择，对每个连接做负载均衡。
   */
  private final Random rand;
  private final int numConnectionsPerPeer;

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;

  public TransportClientFactory(
      TransportContext context,
      List<TransportClientBootstrap> clientBootstraps) {
    this.context = Preconditions.checkNotNull(context);
    this.conf = context.getConf();
    this.clientBootstraps = Lists.newArrayList(Preconditions.checkNotNull(clientBootstraps));
    // 针对每个 Socket 地址的连接池 ClientPool 的缓存。
    this.connectionPool = new ConcurrentHashMap<>();
    // 两个节点间用于获取数据的并发连接数，后面在初始化 ClientPool 时作为 pool 的 size，default 1
    this.numConnectionsPerPeer = conf.numConnectionsPerPeer();
    this.rand = new Random();

    // IO mode: nio or epoll，默认 nio
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    // 客户端 channel 被创建的时候所使用的类，默认 NioSocketChannel
    this.socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
    // TODO: Make thread pool name configurable.
    // 根据 Netty 的规范，客户端只有 worker 组，WorkerGroup 实际类型默认是 NioEventLoopGroup
    this.workerGroup = NettyUtils.createEventLoop(ioMode, conf.clientThreads(), "shuffle-client");
    // 创建一个池化的 ByteBuf 分配器，但禁用线程本地缓存。
    this.pooledAllocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), false /* allowCache */, conf.clientThreads());
  }

  /**
   * Create a {@link TransportClient} connecting to the given remote host / port.
   *
   * We maintains an array of clients (size determined by spark.shuffle.io.numConnectionsPerPeer)
   * and randomly picks one to use. If no client was previously created in the randomly selected
   * spot, this function creates a new client and places it there.
   * 我们维护一个客户端数组（大小由spark.shuffle.io.numConnectionsPerPeer确定）并随机选择一个使用。
   * 如果以前没有在随机选择的位置创建任何客户端，则此函数将创建一个新客户端并将其放置在该位置。
   *
   * Prior to the creation of a new TransportClient, we will execute all
   * {@link TransportClientBootstrap}s that are registered with this factory.
   * 在创建新的TransportClient之前，我们将执行在该工厂注册的所有 TransportClientBootstrap
   *
   * This blocks until a connection is successfully established and fully bootstrapped.
   * 这将阻塞，直到成功建立连接并完全引导为止。
   *
   * Concurrency: This method is safe to call from multiple threads.
   * 并发：从多个线程可以安全地调用此方法。
   */
  public TransportClient createClient(String remoteHost, int remotePort) throws IOException {
    // Get connection from the connection pool first. 首先从连接池获取连接
    // If it is not found or not active, create a new one. 如果找不到或未激活它，则创建一个新的
    // Use unresolved address here to avoid DNS resolution each time we creates a client.
    // 使用 InetSocketAddress 的静态方法 createUnresolved 创建 InetSocketAddress，
    // 这样可以在缓存中已经有 TransportClient 时避免不必要的域名解析
    final InetSocketAddress unresolvedAddress =
      InetSocketAddress.createUnresolved(remoteHost, remotePort);

    // Create the ClientPool if we don't have it yet.
    // 如果还没有，创建 ClientPool
    ClientPool clientPool = connectionPool.get(unresolvedAddress);
    if (clientPool == null) {
      connectionPool.putIfAbsent(unresolvedAddress, new ClientPool(numConnectionsPerPeer));
      clientPool = connectionPool.get(unresolvedAddress);
    }

    // 随机从 ClientPool 中选择一个 TransportClient
    int clientIndex = rand.nextInt(numConnectionsPerPeer);
    TransportClient cachedClient = clientPool.clients[clientIndex];

    // 获取并返回激活的 TransportClient
    if (cachedClient != null && cachedClient.isActive()) {
      // Make sure that the channel will not timeout by updating the last use time of the
      // handler. Then check that the client is still alive, in case it timed out before
      // this code was able to update things.
      // 通过更新处理程序的上次使用时间来确保通道不会超时。
      // 然后检查客户端是否仍在运行，以防万一在此代码无法更新之前超时。
      TransportChannelHandler handler = cachedClient.getChannel().pipeline()
        .get(TransportChannelHandler.class);
      synchronized (handler) {
        handler.getResponseHandler().updateTimeOfLastRequest();
      }

      if (cachedClient.isActive()) {
        logger.trace("Returning cached connection to {}: {}",
          cachedClient.getSocketAddress(), cachedClient);
        return cachedClient;
      }
    }

    // If we reach here, we don't have an existing connection open. Let's create a new one.
    // Multiple threads might race here to create new connections. Keep only one of them active.
    // 如果到达此处，则没有打开的现有连接。让我们创建一个新的。
    // 多个线程可能会竞相在此处创建新的连接。保持其中一个处于活动状态。
    final long preResolveHost = System.nanoTime();
    final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
    final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
    if (hostResolveTimeMs > 2000) {
      logger.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    } else {
      logger.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
    }

    // 前面域名解析产生了竞态条件，此时通过 ClientPool 的 locks 数组进行同步，避免线程安全问题
    synchronized (clientPool.locks[clientIndex]) {
      cachedClient = clientPool.clients[clientIndex];

      if (cachedClient != null) {
        if (cachedClient.isActive()) {
          // 此处表示，已经有其他线程先进来了，所以 cachedClient 处于激活状态
          // 则返回这个已经激活的客户端
          logger.trace("Returning cached connection to {}: {}", resolvedAddress, cachedClient);
          return cachedClient;
        } else {
          logger.info("Found inactive connection to {}, creating a new one.", resolvedAddress);
        }
      }
      // 执行到此处表明线程为第一个进入临界区的线程
      // 则调用重载的 createClient 方法创建新的 TransportClient 对象，并加入 ClientPool
      clientPool.clients[clientIndex] = createClient(resolvedAddress);
      return clientPool.clients[clientIndex];
    }
  }

  /**
   * Create a completely new {@link TransportClient} to the given remote host / port.
   * This connection is not pooled.
   *
   * As with {@link #createClient(String, int)}, this method is blocking.
   */
  public TransportClient createUnmanagedClient(String remoteHost, int remotePort)
      throws IOException {
    final InetSocketAddress address = new InetSocketAddress(remoteHost, remotePort);
    return createClient(address);
  }

  /**
   * Create a completely new {@link TransportClient} to the remote address.
   * 1. 构建根引导程序 Bootstrap 并对其进行设置
   * 2. 为根引导程序设置管道初始化回调函数，此回调函数将调用 TransportContext 的 initializePipeline 方法初始化 Channel 的 pipeline
   * 3. 使用根引导程序连接远程服务器，当连接成功对管道初始化时会回调初始化函数，将 TransportClient 和 Channel 对象分别设置到原子引用 clientRef 和 channelRef 中
   * 4. 给 TransportClient 设置客户端引导程序，即设置 TransportClientFactory 中的 TransportClientBootstrap 列表
   * 5. 返回此 TransportClient 对象
   */
  private TransportClient createClient(InetSocketAddress address) throws IOException {
    logger.debug("Creating new connection to {}", address);

    // 构建根引导程序 Bootstrap 并对其进行设置
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(workerGroup)
      .channel(socketChannelClass)
      // Disable Nagle's Algorithm since we don't want packets to wait 禁用 Nagle 算法，因为我们不希望数据包等待
      .option(ChannelOption.TCP_NODELAY, true)
      .option(ChannelOption.SO_KEEPALIVE, true)
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.connectionTimeoutMs())
      .option(ChannelOption.ALLOCATOR, pooledAllocator);

    final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
    final AtomicReference<Channel> channelRef = new AtomicReference<>();

    // 为根引导程序设置管道初始化回调函数
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) {
        // 调用 TransportContext 的 initializePipeline 方法初始化 Channel 的 pipeline
        TransportChannelHandler clientHandler = context.initializePipeline(ch);
        // 当连接成功对管道初始化时，会回调初始化函数，将 TransportClient 和 Channel 对象分别设置到原子引用 clientRef 和 channelRef 中
        clientRef.set(clientHandler.getClient());
        channelRef.set(ch);
      }
    });

    // Connect to the remote server
    // 使用根引导程序连接远程服务器
    long preConnect = System.nanoTime();
    ChannelFuture cf = bootstrap.connect(address);
    if (!cf.awaitUninterruptibly(conf.connectionTimeoutMs())) {
      throw new IOException(
        String.format("Connecting to %s timed out (%s ms)", address, conf.connectionTimeoutMs()));
    } else if (cf.cause() != null) {
      throw new IOException(String.format("Failed to connect to %s", address), cf.cause());
    }

    TransportClient client = clientRef.get();
    Channel channel = channelRef.get();
    assert client != null : "Channel future completed successfully with null client";

    // Execute any client bootstraps synchronously before marking the Client as successful.
    // 将客户端标记为成功之前，同步执行所有客户端引导程序
    long preBootstrap = System.nanoTime();
    logger.debug("Connection to {} successful, running bootstraps...", address);
    try {
      for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
        // 给 TransportClient 设置客户端引导程序
        clientBootstrap.doBootstrap(client, channel);
      }
    } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
      long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
      logger.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
      client.close();
      throw Throwables.propagate(e);
    }
    long postBootstrap = System.nanoTime();

    logger.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
      address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

    return client;
  }

  /** Close all connections in the connection pool, and shutdown the worker thread pool. */
  @Override
  public void close() {
    // Go through all clients and close them if they are active.
    // 遍历所有客户端，如果它们处于活动状态，则将其关闭。
    for (ClientPool clientPool : connectionPool.values()) {
      for (int i = 0; i < clientPool.clients.length; i++) {
        TransportClient client = clientPool.clients[i];
        if (client != null) {
          // 先将数组引用置为 null，再把 client 对象 close
          clientPool.clients[i] = null;
          JavaUtils.closeQuietly(client);
        }
      }
    }
    connectionPool.clear();

    if (workerGroup != null) {
      workerGroup.shutdownGracefully();
      workerGroup = null;
    }
  }
}
