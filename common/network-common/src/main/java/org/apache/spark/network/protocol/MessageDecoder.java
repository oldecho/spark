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

package org.apache.spark.network.protocol;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decoder used by the client side to encode server-to-client responses.
 * This encoder is stateless so it is safe to be shared by multiple threads.
 * 客户端用于对服务器到客户端响应进行编码的解码器。
 * 此编码器是无状态的，因此可以安全地由多个线程共享。
 *
 * 对从管道中读取的 ByteBuf 进行解析，防止丢包和解析错误。
 *
 * MessageDecoder 与 MessageEncoder 刚好相反，
 * 它会将经过 TransportFrameDecoder 帧解码器解析后得到的 ByteBuf 类型的数据缓冲对象转换为 Message 类型的消息数据
 */
@ChannelHandler.Sharable
public final class MessageDecoder extends MessageToMessageDecoder<ByteBuf> {

  private static final Logger logger = LoggerFactory.getLogger(MessageDecoder.class);

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    // 从 ByteBuf 中获取消息类型
    Message.Type msgType = Message.Type.decode(in);
    // 使用重载的 decode() 方法进行解码
    Message decoded = decode(msgType, in);
    // 检查解码后的消息类型是否正确
    assert decoded.type() == msgType;
    logger.trace("Received message {}: {}", msgType, decoded);
    // 将解码后的消息添加到 out 中
    out.add(decoded);
  }

  private Message decode(Message.Type msgType, ByteBuf in) {
    // 根据消息类型，选择不同类型的消息类，使用它们的静态方法 decode() 进行解码
    switch (msgType) {
      case ChunkFetchRequest:
        return ChunkFetchRequest.decode(in);

      case ChunkFetchSuccess:
        return ChunkFetchSuccess.decode(in);

      case ChunkFetchFailure:
        return ChunkFetchFailure.decode(in);

      case RpcRequest:
        return RpcRequest.decode(in);

      case RpcResponse:
        return RpcResponse.decode(in);

      case RpcFailure:
        return RpcFailure.decode(in);

      case OneWayMessage:
        return OneWayMessage.decode(in);

      case StreamRequest:
        return StreamRequest.decode(in);

      case StreamResponse:
        return StreamResponse.decode(in);

      case StreamFailure:
        return StreamFailure.decode(in);

      default:
        throw new IllegalArgumentException("Unexpected message type: " + msgType);
    }
  }
}
