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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.protocol.RequestMessage;
import org.apache.spark.network.protocol.ResponseMessage;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * The single Transport-level Channel handler which is used for delegating requests to the
 * {@link TransportRequestHandler} and responses to the {@link TransportResponseHandler}.
 * 单个传输级通道处理程序，用于将请求委派给 TransportRequestHandler 和对 TransportResponseHandler 的响应。
 *
 * All channels created in the transport layer are bidirectional. When the Client initiates a Netty
 * Channel with a RequestMessage (which gets handled by the Server's RequestHandler), the Server
 * will produce a ResponseMessage (handled by the Client's ResponseHandler). However, the Server
 * also gets a handle on the same Channel, so it may then begin to send RequestMessages to the
 * Client.
 * This means that the Client also needs a RequestHandler and the Server needs a ResponseHandler,
 * for the Client's responses to the Server's requests.
 * 在传输层中创建的所有通道都是双向的。
 * 当客户端使用 RequestMessage（由服务器的 RequestHandler 处理）启动 Netty 通道时，服务器将生成 ResponseMessage（由客户端的 ResponseHandler 处理）。
 * 但是，服务器也在同一 Channel 上获得了一个句柄，因此它可能随后开始向 Client 发送 RequestMessages。
 * 这意味着客户端还需要一个 RequestHandler，服务器也需要一个 ResponseHandler，以便客户端对服务器请求的响应。
 *
 * This class also handles timeouts from a {@link io.netty.handler.timeout.IdleStateHandler}.
 * We consider a connection timed out if there are outstanding fetch or RPC requests but no traffic
 * on the channel for at least `requestTimeoutMs`. Note that this is duplex traffic; we will not
 * timeout if the client is continuously sending but getting no responses, for simplicity.
 * 此类还处理来自 io.netty.handler.timeout.IdleStateHandler 的超时。
 * 如果至少有 `requestTimeoutMs`，则在通道上有未完成的提取或 RPC 请求但没有流量时，我们认为连接超时。
 * 请注意，这是双工流量。为简单起见，如果客户端持续发送但没有响应，我们不会超时。
 *
 * 代理由 TransportRequestHandler 处理的请求和由 TransportResponseHandler 处理的响应，并加入传输层的处理。
 */
public class TransportChannelHandler extends SimpleChannelInboundHandler<Message> {
  private static final Logger logger = LoggerFactory.getLogger(TransportChannelHandler.class);

  private final TransportClient client;

  /** 代理的响应处理器和请求处理器. */
  private final TransportResponseHandler responseHandler;
  private final TransportRequestHandler requestHandler;
  /** 超时时间（纳秒）. */
  private final long requestTimeoutNs;
  /** 是否关闭空闲连接. */
  private final boolean closeIdleConnections;

  public TransportChannelHandler(
      TransportClient client,
      TransportResponseHandler responseHandler,
      TransportRequestHandler requestHandler,
      long requestTimeoutMs,
      boolean closeIdleConnections) {
    this.client = client;
    this.responseHandler = responseHandler;
    this.requestHandler = requestHandler;
    this.requestTimeoutNs = requestTimeoutMs * 1000L * 1000;
    this.closeIdleConnections = closeIdleConnections;
  }

  public TransportClient getClient() {
    return client;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    logger.warn("Exception in connection from " + getRemoteAddress(ctx.channel()),
      cause);
    requestHandler.exceptionCaught(cause);
    responseHandler.exceptionCaught(cause);
    ctx.close();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    try {
      requestHandler.channelActive();
    } catch (RuntimeException e) {
      logger.error("Exception from request handler while registering channel", e);
    }
    try {
      responseHandler.channelActive();
    } catch (RuntimeException e) {
      logger.error("Exception from response handler while registering channel", e);
    }
    super.channelRegistered(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    try {
      requestHandler.channelInactive();
    } catch (RuntimeException e) {
      logger.error("Exception from request handler while unregistering channel", e);
    }
    try {
      responseHandler.channelInactive();
    } catch (RuntimeException e) {
      logger.error("Exception from response handler while unregistering channel", e);
    }
    super.channelUnregistered(ctx);
  }

  /**
   * 当 TransportChannelHandler 读取的 request 是 RequestMessage 时，则将此消息的处理进一步处理交给 TransportRequestHandler，
   * 当读取的 request 是 ResponseMessage 时，则将此消息的处理进一步交给 TransportResponseHandler
   */
  @Override
  public void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
    if (request instanceof RequestMessage) {
      requestHandler.handle((RequestMessage) request);
    } else {
      responseHandler.handle((ResponseMessage) request);
    }
  }

  /** Triggered based on events from an {@link io.netty.handler.timeout.IdleStateHandler}. */
  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      // See class comment for timeout semantics. In addition to ensuring we only timeout while
      // there are outstanding requests, we also do a secondary consistency check to ensure
      // there's no race between the idle timeout and incrementing the numOutstandingRequests
      // (see SPARK-7003).
      //
      // To avoid a race between TransportClientFactory.createClient() and this code which could
      // result in an inactive client being returned, this needs to run in a synchronized block.
      synchronized (this) {
        boolean isActuallyOverdue =
          System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
        if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
          if (responseHandler.numOutstandingRequests() > 0) {
            String address = getRemoteAddress(ctx.channel());
            logger.error("Connection to {} has been quiet for {} ms while there are outstanding " +
              "requests. Assuming connection is dead; please adjust spark.network.timeout if " +
              "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
            client.timeOut();
            ctx.close();
          } else if (closeIdleConnections) {
            // While CloseIdleConnections is enable, we also close idle connection
            client.timeOut();
            ctx.close();
          }
        }
      }
    }
    ctx.fireUserEventTriggered(evt);
  }

  public TransportResponseHandler getResponseHandler() {
    return responseHandler;
  }

}
