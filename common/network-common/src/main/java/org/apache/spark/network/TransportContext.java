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

package org.apache.spark.network;

import java.util.List;

import com.google.common.collect.Lists;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Contains the context to create a {@link TransportServer}, {@link TransportClientFactory}, and to
 * setup Netty Channel pipelines with a
 * {@link org.apache.spark.network.server.TransportChannelHandler}.
 *
 * 包含上下文，以创建 TransportServer，TransportClientFactory
 * 并使用 org.apache.spark.network.server.TransportChannelHandler 设置 Netty Channel 管道。
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * TransportClient 提供了两种通信协议，即控制平面RPC和数据平面“块获取”。
 * RPC 的处理是在 TransportContext 的范围之外执行的（即，由用户提供的处理程序），
 * 它负责设置流，这些流可以使用零拷贝IO以块的形式流经数据平面。
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 *
 * TransportServer 和 TransportClientFactory 都为每个通道创建一个 TransportChannelHandler。
 * 由于每个 TransportChannelHandler 都包含一个 TransportClient，因此服务器进程可以通过现有通道将消息发送回客户端。
 *
 * 传输上下文，包含了用于创建 TransportServer & TransportClientFactory 的上下文信息，并支持使用 TransportChannelHandler
 * 设置 Netty 提供的 SocketChannel 的 Pipeline 的实现。
 */
public class TransportContext {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  /** 传输上下文的配置信息 */
  private final TransportConf conf;
  /** 发送消息的处理器 */
  private final RpcHandler rpcHandler;
  /** 标记是否关闭空闲连接 */
  private final boolean closeIdleConnections;

  /** 消息编码器 */
  private final MessageEncoder encoder;
  /** 消息解码器 */
  private final MessageDecoder decoder;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.encoder = new MessageEncoder();
    this.decoder = new MessageDecoder();
    this.closeIdleConnections = closeIdleConnections;
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   * 通过指定的客户端引导程序初始化一个客户端工厂，通过客户端工厂新建一个客户端。
   * 引导程序将被同步执行，并且必须成功运行才能创建客户端。
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(Lists.<TransportClientBootstrap>newArrayList());
  }

  /**
   * Create a server which will attempt to bind to a specific port.
   * 创建一个将尝试绑定到特定端口的服务器。
   */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /**
   * Create a server which will attempt to bind to a specific host and port.
   * 创建将尝试绑定到特定主机和端口的服务器。
   */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /**
   * Creates a new server, binding to any available ephemeral port.
   * 创建一个新服务器，绑定到任何可用的临时端口。
   */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  /** 创建一个新服务器，绑定到任何可用的临时端口。 */
  public TransportServer createServer() {
    return createServer(0, Lists.<TransportServerBootstrap>newArrayList());
  }

  /** 初始化客户端/服务器 Netty Channel Pipeline. */
  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * Initializes a client or server Netty Channel Pipeline which encodes/decodes messages and
   * has a {@link org.apache.spark.network.server.TransportChannelHandler} to handle request or
   * response messages.
   *
   * 初始化客户端/服务器 Netty Channel Pipeline，
   * 该通道对消息进行编码/解码，并且具有一个 TransportChannelHandler 对象来处理请求或响应消息。
   *
   * 1. 调用 createChannelHandler 方法创建 TransportChannelHandler，从 createChannelHandler 的实现可以看到，
   *    真正创建 TransportClient 是在这里发生的。
   *    通过 TransportClient 的构造过程看到 RpcHandler 与 TransportClient 毫无关系，TransportClient 只使用了 TransportResponseHandler。
   *    TransportResponseHandler 在服务端将代理 TransportRequestHandler 对请求消息进行处理，
   *    并在客户端代理 TransportResponseHandler 对响应消息进行处理。
   * 2. 对管道进行设置，这里的 ENCODER（即 MessageEncoder）派生自 Netty 的 ChannelOutboundHandler 接口；
   *    DECODER（即 MessageDecoder）、TransportChannelHandler 及 TransportFrameDecoder（由工具类 NettyUtils 的静态方法
   *    createFrameDecoder 创建）派生自 Netty 的 ChannelInboundHandler 接口；
   *    IdleStateHandler 同时实现了 ChannelOutboundHandler 和 ChannelInboundHandler 接口。
   *    根据 Netty 的 API 行为，通过 addLast 方法注册多个 Handler 时，ChannelInboundHandler 按照注册的先后顺序执行，
   *    ChannelOutboundHandler 按照注册的先后顺序逆序执行，因此在管道两端（无论时服务端还是客户端）处理请求和响应的流程如下
   *
   *    ---------------------外界节点-------------------------
   *    |  request                             response    |
   *    |--------------------------------------------------|
   *    |     ⬇                                           |
   *    | TransportFrameDecoder                   ⬆       |
   *    |          ⬇                                      |
   *    |   MessageDecoder                MessageEncoder   |
   *    |                  ⬇              ⬆               |
   *    |                   IdleStateHandler               |
   *    |                 ⬇                               |
   *    | TransportChannelHandler                          |
   *    |            ⬇                    ⬆               |
   *    |-------------------ChannelPipeline----------------|
   *    |==================================================|
   *    |___________________Netty I/O Thread_______________|
   *
   * @param channel The channel to initialize.
   * @param channelRpcHandler The RPC handler to use for the channel.
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   * 返回创建的 TransportChannelHandler，其中包括一个可用于在此通道上进行通信的 TransportClient。
   * TransportClient 直接与 ChannelHandler 关联，以确保同一通道的所有用户都获得相同的 TransportClient 对象。
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      // 创建 TransportChannelHandler
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      // 使用 addLast(...) 按顺序，添加 Handler
      channel.pipeline()
        .addLast("encoder", encoder)  // 消息编码，MessageEncoder，ChannelOutboundHandler
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())  // 帧解码器 ChannelInboundHandler
        .addLast("decoder", decoder)  // 消息解码，MessageDecoder，ChannelInboundHandler
        // 心跳检测，ChannelOutboundHandler，ChannelInboundHandler，只监听了读写空闲，默认超时时长为 120s。
        .addLast("idleStateHandler", new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        // 注意：当前保证将按请求顺序返回块，但是如果这不是同一事件循环的一部分，则需要更多的逻辑来保证。
        .addLast("handler", channelHandler);  // InboundHandler
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * Creates the server- and client-side handler which is used to handle both RequestMessages and
   * ResponseMessages. The channel is expected to have been successfully created, though certain
   * properties (such as the remoteAddress()) may not be available yet.
   *
   * 创建服务器端和客户端处理程序，用于处理 RequestMessages 和 ResponseMessages。
   * 尽管某些属性（例如remoteAddress()）可能尚不可用，但仍有望成功创建通道。
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    // 真正创建 TransportClient 的地方，并且 TransportClient 与 RpcHandler 没有关系
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler);
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections);
  }

  public TransportConf getConf() { return conf; }
}
