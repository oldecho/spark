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

package org.apache.spark.network.sasl;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.security.sasl.Sasl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.StreamManager;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

/**
 * RPC Handler which performs SASL authentication before delegating to a child RPC handler.
 * The delegate will only receive messages if the given connection has been successfully
 * authenticated. A connection may be authenticated at most once.
 * 在委派给子RPC处理程序之前执行SASL身份验证的RPC处理程序。
 * 仅当给定的连接已成功通过身份验证时，委托方才会接收消息。连接最多可以认证一次。
 *
 * Note that the authentication process consists of multiple challenge-response pairs, each of
 * which are individual RPCs.
 * 请注意，认证过程由多个质询-响应对组成，每个对都是独立的RPC。
 */
class SaslRpcHandler extends RpcHandler {
  private static final Logger logger = LoggerFactory.getLogger(SaslRpcHandler.class);

  /** Transport configuration. */
  private final TransportConf conf;

  /** The client channel. */
  private final Channel channel;

  /**
   * RpcHandler we will delegate to for authenticated connections.
   * 我们用来委托进行身份认证的 RpcHandler
   */
  private final RpcHandler delegate;

  /** Class which provides secret keys which are shared by server and client on a per-app basis. */
  private final SecretKeyHolder secretKeyHolder;

  private SparkSaslServer saslServer;

  /**
   * isComplete 字段用于标记记录是否已经通过了SASL认证
   * 如果为true，则直接将处理委托给RpcHandler，否则进行认证操作，一旦认证成功则会将 isComplete 字段置为true，往后的请求就不需要再次认证了
   */
  private boolean isComplete;

  /** 构造函数. */
  SaslRpcHandler(
      TransportConf conf,
      Channel channel,
      RpcHandler delegate,
      SecretKeyHolder secretKeyHolder) {
    this.conf = conf;
    this.channel = channel;
    this.delegate = delegate;
    this.secretKeyHolder = secretKeyHolder;
    this.saslServer = null;
    this.isComplete = false;
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
    if (isComplete) {  // 已经完成了SASL认证交换
      // Authentication complete, delegate to base handler.
      // 将消息传递给SaslRpcHandler所代理的下游RpcHandler并返回
      delegate.receive(client, message, callback);
      return;
    }

    /*
    实现sasl认证流程
     */
    // 将 ByteBuffer 转换为 Netty 的 ByteBuf
    ByteBuf nettyBuf = Unpooled.wrappedBuffer(message);
    // 用于记录SASL解密后的消息
    SaslMessage saslMessage;
    try {
      // 对客户端发送的消息进行SASL解密
      saslMessage = SaslMessage.decode(nettyBuf);
    } finally {
      // 释放
      nettyBuf.release();
    }

    // 如果 saslServer 未创建，则需要创建 SparkSaslServer
    if (saslServer == null) {
      // First message in the handshake, setup the necessary state.
      client.setClientId(saslMessage.appId);
      saslServer = new SparkSaslServer(saslMessage.appId, secretKeyHolder,
        conf.saslServerAlwaysEncrypt());
    }

    byte[] response;
    try {
      // 使用 SparkSaslServer 处理已解密的消息
      response = saslServer.response(JavaUtils.bufferToArray(
        saslMessage.body().nioByteBuffer()));
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    // 处理成功，调用回调的 onSuccess() 方法
    callback.onSuccess(ByteBuffer.wrap(response));

    // Setup encryption after the SASL response is sent, otherwise the client can't parse the
    // response. It's ok to change the channel pipeline here since we are processing an incoming
    // message, so the pipeline is busy and no new incoming messages will be fed to it before this
    // method returns. This assumes that the code ensures, through other means, that no outbound
    // messages are being written to the channel while negotiation is still going on.
    // 发送SASL响应后设置加密，否则客户端无法解析响应。
    // 由于我们正在处理传入的消息，因此可以在此处更改通道管道，因此，该管道很忙，在此方法返回之前，不会有新的传入消息被馈送给它。
    // 假定代码通过其他方式确保在协商仍在进行时没有出站消息被写入通道。
    if (saslServer.isComplete()) {
      logger.debug("SASL authentication successful for channel {}", client);
      // SASL认证交换已经完成，置 isComplete 标记为 true
      isComplete = true;
      if (SparkSaslServer.QOP_AUTH_CONF.equals(saslServer.getNegotiatedProperty(Sasl.QOP))) {
        logger.debug("Enabling encryption for channel {}", client);
        SaslEncryption.addToChannel(channel, saslServer, conf.maxSaslEncryptedBlockSize());
        saslServer = null;
      } else {
        saslServer.dispose();
        saslServer = null;
      }
    }
  }

  @Override
  public void receive(TransportClient client, ByteBuffer message) {
    delegate.receive(client, message);
  }

  @Override
  public StreamManager getStreamManager() {
    return delegate.getStreamManager();
  }

  @Override
  public void channelActive(TransportClient client) {
    delegate.channelActive(client);
  }

  @Override
  public void channelInactive(TransportClient client) {
    try {
      delegate.channelInactive(client);
    } finally {
      if (saslServer != null) {
        saslServer.dispose();
      }
    }
  }

  @Override
  public void exceptionCaught(Throwable cause, TransportClient client) {
    delegate.exceptionCaught(cause, client);
  }

}
