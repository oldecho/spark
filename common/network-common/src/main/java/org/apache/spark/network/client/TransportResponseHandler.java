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

import java.io.IOException;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.protocol.ChunkFetchFailure;
import org.apache.spark.network.protocol.ChunkFetchSuccess;
import org.apache.spark.network.protocol.ResponseMessage;
import org.apache.spark.network.protocol.RpcFailure;
import org.apache.spark.network.protocol.RpcResponse;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamFailure;
import org.apache.spark.network.protocol.StreamResponse;
import org.apache.spark.network.server.MessageHandler;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * Handler that processes server responses, in response to requests issued from a
 * [[TransportClient]]. It works by tracking the list of outstanding requests (and their callbacks).
 * 处理服务器响应以响应 TransportClient 发出的请求的处理程序。它通过跟踪未完成的请求（及其回调）列表来工作。
 *
 * Concurrency: thread safe and can be called from multiple threads.
 * 并发：线程安全，可以从多个线程中调用。
 *
 * 用于处理服务端的响应，并且对发出请求的苦护短进行响应的处理程序。
 */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {
  private static final Logger logger = LoggerFactory.getLogger(TransportResponseHandler.class);

  private final Channel channel;

  /** 存放 ChunkFetchRequest 请求对应的回调. */
  private final Map<StreamChunkId, ChunkReceivedCallback> outstandingFetches;

  /** 存放 RpcRequest 请求对应的回调. */
  private final Map<Long, RpcResponseCallback> outstandingRpcs;

  /** 存放 StreamRequest 请求对应的回调. */
  private final Queue<StreamCallback> streamCallbacks;
  private volatile boolean streamActive;

  /**
   * Records the time (in system nanoseconds) that the last fetch or RPC request was sent.
   * 记录发送上一个获取或RPC请求的时间（以系统纳秒为单位）。
   */
  private final AtomicLong timeOfLastRequestNs;

  /** 构造函数. */
  public TransportResponseHandler(Channel channel) {
    this.channel = channel;
    this.outstandingFetches = new ConcurrentHashMap<>();
    this.outstandingRpcs = new ConcurrentHashMap<>();
    this.streamCallbacks = new ConcurrentLinkedQueue<>();
    this.timeOfLastRequestNs = new AtomicLong(0);
  }

  /** 添加 ChunkFetchRequest 请求对应回调. */
  public void addFetchRequest(StreamChunkId streamChunkId, ChunkReceivedCallback callback) {
    // 将更新最后一次请求的时间为当前系统时间
    updateTimeOfLastRequest();
    // 将 streamChunkId 和对应的 ChunkReceivedCallback 回调存入 outstandingRpcs（ConcurrentHashMap）
    outstandingFetches.put(streamChunkId, callback);
  }

  /** 移除 ChunkFetchRequest 请求对应回调. */
  public void removeFetchRequest(StreamChunkId streamChunkId) {
    outstandingFetches.remove(streamChunkId);
  }

  /** 添加 RpcRequest 请求对应回调. */
  public void addRpcRequest(long requestId, RpcResponseCallback callback) {
    // 更新最后一次请求的时间为当前系统时间
    updateTimeOfLastRequest();
    // 将 requestId 和对应的 RpcResponseCallback 回调存入 outstandingRpcs（ConcurrentHashMap）
    outstandingRpcs.put(requestId, callback);
  }

  /** 移除 RpcRequest 请求对应回调. */
  public void removeRpcRequest(long requestId) {
    outstandingRpcs.remove(requestId);
  }

  /** 添加 StreamRequest 请求对应回调. */
  public void addStreamCallback(StreamCallback callback) {
    timeOfLastRequestNs.set(System.nanoTime());
    streamCallbacks.offer(callback);
  }

  /** 标识 streamActive 为非激活状态. */
  @VisibleForTesting
  public void deactivateStream() {
    streamActive = false;
  }

  /**
   * Fire the failure callback for all outstanding requests. This is called when we have an
   * uncaught exception or pre-mature connection termination.
   * 对所有未完成的请求触发失败回调。当我们有一个未捕获的异常或过早的连接终止时，将调用此方法。
   */
  private void failOutstandingRequests(Throwable cause) {
    for (Map.Entry<StreamChunkId, ChunkReceivedCallback> entry : outstandingFetches.entrySet()) {
      entry.getValue().onFailure(entry.getKey().chunkIndex, cause);
    }
    for (Map.Entry<Long, RpcResponseCallback> entry : outstandingRpcs.entrySet()) {
      entry.getValue().onFailure(cause);
    }

    // It's OK if new fetches appear, as they will fail immediately.
    outstandingFetches.clear();
    outstandingRpcs.clear();
  }

  @Override
  public void channelActive() {
  }

  @Override
  public void channelInactive() {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(new IOException("Connection from " + remoteAddress + " closed"));
    }
  }

  @Override
  public void exceptionCaught(Throwable cause) {
    if (numOutstandingRequests() > 0) {
      String remoteAddress = getRemoteAddress(channel);
      logger.error("Still have {} requests outstanding when connection from {} is closed",
        numOutstandingRequests(), remoteAddress);
      failOutstandingRequests(cause);
    }
  }

  @Override
  public void handle(ResponseMessage message) throws Exception {
    if (message instanceof ChunkFetchSuccess) {  // 块获取请求成功的响应
      // 转换消息类型
      ChunkFetchSuccess resp = (ChunkFetchSuccess) message;
      // 根据响应中的 streamChunkId 从 outstandingRpcs 中获取对应的回调监听器
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel));
        resp.body().release();
      } else {
        // 先将其从 outstandingFetches 中移除
        outstandingFetches.remove(resp.streamChunkId);
        // 调用其 onSuccess() 回调方法
        listener.onSuccess(resp.streamChunkId.chunkIndex, resp.body());
        resp.body().release();
      }
    } else if (message instanceof ChunkFetchFailure) {  // 块获取请求失败的响应
      // 转换消息类型
      ChunkFetchFailure resp = (ChunkFetchFailure) message;
      // 根据响应中的 streamChunkId 从 outstandingRpcs 中获取对应的回调监听器
      ChunkReceivedCallback listener = outstandingFetches.get(resp.streamChunkId);
      if (listener == null) {
        logger.warn("Ignoring response for block {} from {} ({}) since it is not outstanding",
          resp.streamChunkId, getRemoteAddress(channel), resp.errorString);
      } else {
        // 先将其从 outstandingRpcs 中移除
        outstandingFetches.remove(resp.streamChunkId);
        // 调用其 onFailure() 方法
        listener.onFailure(resp.streamChunkId.chunkIndex, new ChunkFetchFailureException(
          "Failure while fetching " + resp.streamChunkId + ": " + resp.errorString));
      }
    } else if (message instanceof RpcResponse) {  // RPC请求成功的响应
      // 转换消息类型
      RpcResponse resp = (RpcResponse) message;
      // 根据响应中的 requestId 从 outstandingRpcs 中获取对应的回调监听器
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.body().size());
      } else {
        // 先将其从 outstandingRpcs 中移除
        outstandingRpcs.remove(resp.requestId);
        try {
          // 调用其 onSuccess() 方法
          listener.onSuccess(resp.body().nioByteBuffer());
        } finally {
          resp.body().release();
        }
      }
    } else if (message instanceof RpcFailure) {  // RPC请求失败的响应
      // 转换消息类型
      RpcFailure resp = (RpcFailure) message;
      RpcResponseCallback listener = outstandingRpcs.get(resp.requestId);
      if (listener == null) {
        logger.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
          resp.requestId, getRemoteAddress(channel), resp.errorString);
      } else {
        outstandingRpcs.remove(resp.requestId);
        listener.onFailure(new RuntimeException(resp.errorString));
      }
    } else if (message instanceof StreamResponse) {  // 流获取请求成功的响应
      // 转换消息类型
      StreamResponse resp = (StreamResponse) message;
      StreamCallback callback = streamCallbacks.poll();
      if (callback != null) {
        // 根据 StreamResponse 消息的 byteCount 字段决定是否给处理器链中的 TransportFrameDecoder 帧解码器设置 StreamInterceptor，
        // 这个操作是为了接收接下来的流数据
        if (resp.byteCount > 0) {
          StreamInterceptor interceptor = new StreamInterceptor(this, resp.streamId, resp.byteCount,
            callback);
          try {
            TransportFrameDecoder frameDecoder = (TransportFrameDecoder)
              channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME);
            frameDecoder.setInterceptor(interceptor);
            streamActive = true;
          } catch (Exception e) {
            logger.error("Error installing stream handler.", e);
            deactivateStream();
          }
        } else {
          try {
            callback.onComplete(resp.streamId);
          } catch (Exception e) {
            logger.warn("Error in stream handler onComplete().", e);
          }
        }
      } else {
        logger.error("Could not find callback for StreamResponse.");
      }
    } else if (message instanceof StreamFailure) {  // 流获取请求失败的响应
      // 转换消息类型
      StreamFailure resp = (StreamFailure) message;
      StreamCallback callback = streamCallbacks.poll();
      if (callback != null) {
        try {
          callback.onFailure(resp.streamId, new RuntimeException(resp.error));
        } catch (IOException ioe) {
          logger.warn("Error in stream failure handler.", ioe);
        }
      } else {
        logger.warn("Stream failure with unknown callback: {}", resp.error);
      }
    } else {
      throw new IllegalStateException("Unknown response type: " + message.type());
    }
  }

  /** Returns total number of outstanding requests (fetch requests + rpcs) */
  public int numOutstandingRequests() {
    return outstandingFetches.size() + outstandingRpcs.size() + streamCallbacks.size() +
      (streamActive ? 1 : 0);
  }

  /** Returns the time in nanoseconds of when the last request was sent out. */
  public long getTimeOfLastRequestNs() {
    return timeOfLastRequestNs.get();
  }

  /** Updates the time of the last request to the current system time. */
  public void updateTimeOfLastRequest() {
    timeOfLastRequestNs.set(System.nanoTime());
  }

}
