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

package org.apache.spark.network.server;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.apache.spark.network.util.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.TransportContext;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * Server for the efficient, low-level streaming service.
 * RPC 框架的服务端，提供高效的、低级别的流服务。
 *
 * TransportServer 实现了 Spark 节点接收消息的服务端，内部使用的其实 Netty 的 ServerBootstrap
 */
public class TransportServer implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportServer.class);

  private final TransportContext context;
  private final TransportConf conf;
  /** 发送消息的处理器. */
  private final RpcHandler appRpcHandler;
  /** 服务端传输引导器列表，该列表的引导器会在服务端 Channel 初始化时执行特定方法. */
  private final List<TransportServerBootstrap> bootstraps;

  /** Netty's ServerBootstrap. */
  private ServerBootstrap bootstrap;
  /** Netty's ChannelFuture. */
  private ChannelFuture channelFuture;
  private int port = -1;

  /**
   * Creates a TransportServer that binds to the given host and the given port, or to any available
   * if 0. If you don't want to bind to any special host, set "hostToBind" to null.
   * 创建一个 TransportServer，该 Server 绑定到给定的主机和给定的端口，或者绑定到任何可用的（如果 port 为 0）。
   * 如果您不想绑定到任何特殊的主机，请将 “hostToBind” 设置为 null
   * */
  public TransportServer(
      TransportContext context,
      String hostToBind,
      int portToBind,
      RpcHandler appRpcHandler,
      List<TransportServerBootstrap> bootstraps) {
    this.context = context;
    this.conf = context.getConf();
    this.appRpcHandler = appRpcHandler;
    this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(bootstraps));

    try {
      init(hostToBind, portToBind);
    } catch (RuntimeException e) {
      JavaUtils.closeQuietly(this);
      throw e;
    }
  }

  public int getPort() {
    if (port == -1) {
      throw new IllegalStateException("Server not initialized");
    }
    return port;
  }

  /**
   * 1. 创建 bossGroup & workerGroup
   * 2. 创建一个汇集 ByteBuf 但对本地线程缓存禁用的分配器
   * 3. 调用 Netty 的 API 创建 Netty 的服务端根引导程序并对其进行配置
   * 4. 为根引导程序设置管道初始化回调函数，此回调函数首先设置 TransportServerBootstrap 到根引导程序中，
   *    然后调用 TransportContext 的 initializePipeline 方法初始化 Channel 的 pipeline
   * 5. 给根引导程序绑定 Socket 的监听端口，最后返回监听的端口
   */
  private void init(String hostToBind, int portToBind) {
    // 根据 Netty 的 API 文档，Netty 服务端需同时创建 bossGroup 和 workerGroup
    IOMode ioMode = IOMode.valueOf(conf.ioMode());
    EventLoopGroup bossGroup =
      NettyUtils.createEventLoop(ioMode, conf.serverThreads(), "shuffle-server");
    EventLoopGroup workerGroup = bossGroup;

    // 创建一个汇集 ByteBuf 但对本地线程缓存禁用的分配器
    PooledByteBufAllocator allocator = NettyUtils.createPooledByteBufAllocator(
      conf.preferDirectBufs(), true /* allowCache */, conf.serverThreads());

    // 创建 Netty 的服务端根引导程序并对其进行配置
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(NettyUtils.getServerChannelClass(ioMode))
      .option(ChannelOption.ALLOCATOR, allocator)
      .childOption(ChannelOption.ALLOCATOR, allocator);

    if (conf.backLog() > 0) {
      bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
    }

    if (conf.receiveBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
    }

    if (conf.sendBuf() > 0) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
    }

    // 为根引导程序设置管道初始化回调函数
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      // 当服务端的 Channel 初始化时该方法会被调用
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        RpcHandler rpcHandler = appRpcHandler;
        // 遍历所有的 TransportServerBootstrap，调用其 doBootstraps() 方法
        for (TransportServerBootstrap bootstrap : bootstraps) {
          rpcHandler = bootstrap.doBootstrap(ch, rpcHandler);
        }
        // 使用 TransportContext 的 initializePipeline() 方法为服务端的 ChannelPipeline 添加处理器
        context.initializePipeline(ch, rpcHandler);
      }
    });

    // 给根引导程序绑定 Socket 的监听端口
    InetSocketAddress address = hostToBind == null ?
        new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
    channelFuture = bootstrap.bind(address);
    channelFuture.syncUninterruptibly();

    // 记录绑定端口
    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    logger.debug("Shuffle server started on port: {}", port);
  }

  @Override
  public void close() {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      channelFuture = null;
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully();
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully();
    }
    bootstrap = null;
  }
}
