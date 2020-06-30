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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.protocol.ChunkFetchRequest;
import org.apache.spark.network.protocol.OneWayMessage;
import org.apache.spark.network.protocol.RpcRequest;
import org.apache.spark.network.protocol.StreamChunkId;
import org.apache.spark.network.protocol.StreamRequest;
import static org.apache.spark.network.util.NettyUtils.getRemoteAddress;

/**
 * Client for fetching consecutive chunks of a pre-negotiated stream. This API is intended to allow
 * efficient transfer of a large amount of data, broken up into chunks with size ranging from
 * hundreds of KB to a few MB.
 * RPC 框架的客户端，用于获取预先协商好的流中的连续块。
 * TransportClient 旨在允许有效传输的最大数据，这些数据将被拆分成几百 KB 到几 MB 的块。
 *
 * Note that while this client deals with the fetching of chunks from a stream (i.e., data plane),
 * the actual setup of the streams is done outside the scope of the transport layer. The convenience
 * method "sendRPC" is provided to enable control plane communication between the client and server
 * to perform this setup.
 * 当 TransportClient 处理六种获取的块时，实际的设置实在传输层之外完成的。
 * sendRPC 方法能够在客户端和服务端的同一水平线的通信进行这些设置。
 *
 * For example, a typical workflow might be:
 * client.sendRPC(new OpenFile("/foo")) --&gt; returns StreamId = 100
 * client.fetchChunk(streamId = 100, chunkIndex = 0, callback)
 * client.fetchChunk(streamId = 100, chunkIndex = 1, callback)
 * ...
 * client.sendRPC(new CloseStream(100))
 *
 * Construct an instance of TransportClient using {@link TransportClientFactory}. A single
 * TransportClient may be used for multiple streams, but any given stream must be restricted to a
 * single client, in order to avoid out-of-order responses.
 * 使用 TransportClientFactory 构造 TransportClient 的实例。
 * 单个 TransportClient 可以用于多个流，但是任何给定的流都必须限制为一个客户端，以避免混乱的响应。
 *
 * NB: This class is used to make requests to the server, while {@link TransportResponseHandler} is
 * responsible for handling responses from the server.
 * 此类用于向服务器发出请求，而 TransportResponseHandler 负责处理来自服务器的响应。
 *
 * Concurrency: thread safe and can be called from multiple threads.
 * 并发：线程安全，可以从多个线程中调用。
 */
public class TransportClient implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportClient.class);

  /** 进行通信的 Channel 通道对象. */
  private final Channel channel;
  /** 响应处理器. */
  private final TransportResponseHandler handler;
  /** 客户端ID. */
  @Nullable private String clientId;
  /** 标记是否超时. */
  private volatile boolean timedOut;

  public TransportClient(Channel channel, TransportResponseHandler handler) {
    this.channel = Preconditions.checkNotNull(channel);
    this.handler = Preconditions.checkNotNull(handler);
    this.timedOut = false;
  }

  public Channel getChannel() {
    return channel;
  }

  public boolean isActive() {
    return !timedOut && (channel.isOpen() || channel.isActive());
  }

  public SocketAddress getSocketAddress() {
    return channel.remoteAddress();
  }

  /**
   * Returns the ID used by the client to authenticate itself when authentication is enabled.
   *
   * @return The client ID, or null if authentication is disabled.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Sets the authenticated client ID. This is meant to be used by the authentication layer.
   *
   * Trying to set a different client ID after it's been set will result in an exception.
   */
  public void setClientId(String id) {
    Preconditions.checkState(clientId == null, "Client ID has already been set.");
    this.clientId = id;
  }

  /**
   * Requests a single chunk from the remote side, from the pre-negotiated streamId.
   * 从远程协商的 streamId 请求单个块。
   *
   * Chunk indices go from 0 onwards. It is valid to request the same chunk multiple times, though
   * some streams may not support this.
   * 块索引从 0 开始。多次请求相同的块是有效的，尽管有些流可能不支持此功能。
   *
   * Multiple fetchChunk requests may be outstanding simultaneously, and the chunks are guaranteed
   * to be returned in the same order that they were requested, assuming only a single
   * TransportClient is used to fetch the chunks.
   * 多个 fetchChunk 请求可能同时未完成，并且假定仅使用一个 TransportClient 来提取块，并确保这些块以与请求时相同的顺序返回。
   *
   * 用于发送 ChunkFetchRequest 消息，ChunkFetchRequest 消息用于拉取数据块。
   *
   * @param streamId Identifier that refers to a stream in the remote StreamManager. This should
   *                 be agreed upon by client and server beforehand.
   * @param chunkIndex 0-based index of the chunk to fetch
   * @param callback Callback invoked upon successful receipt of chunk, or upon any failure.
   */
  public void fetchChunk(
      long streamId,  // 流ID
      final int chunkIndex,  // 块索引
      final ChunkReceivedCallback callback) {  // 响应回调处理器
    /*
    fetchChunk 方法除了存放回调对象的方式和构造的消息对象类型与 sendRpc 方法不同以外，功能实现上几乎是一致的。
    对于 ChunkReceivedCallback 类型的回调对象，存放位置是 TransportResponseHandler 的 outstandingFetches 字典，键为 StreamChunkId 对象；
    另外 fetchChunk 方法最终构造的消息对象类型是 ChunkFetchRequest。
     */
    // 记录开始处理时间
    final long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
    }

    // 根据 streamId 和 chunkIndex 创建 StreamChunkId 对象
    final StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
    // 向 TransportResponseHandler 的 outstandingFetches 字典添加 streamChunkId 和 ChunkReceivedCallback 的引用关系
    handler.addFetchRequest(streamChunkId, callback);

    // 发送 ChunkFetchRequest 请求
    channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(
      new ChannelFutureListener() {  // 添加了汇报发送情况的回调监听器
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {  // 发送成功
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", streamChunkId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {  // 发送失败
            String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            // 从 TransportResponseHandler 中移除对应的 ChunkReceivedCallback 回调
            handler.removeFetchRequest(streamChunkId);
            // 关闭 Channel
            channel.close();
            try {
              callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });
  }

  /**
   * Request to stream the data with the given stream ID from the remote end.
   * 从远端请求以给定的 stream ID 传输数据。
   *
   * 用于发送 StreamRequest 消息。
   * StreamRequest 消息用于请求拉取源源不断的数据流，由于 StreamRequest 的响应是数据流，因此可能会分多次返回。
   *
   * @param streamId The stream to fetch.
   * @param callback Object to call with the stream data.
   */
  public void stream(final String streamId, final StreamCallback callback) {
    /*
    stream 方法中存放回调的结构是队列（TransportResponseHandler的streamCallbacks队列）。
    客户端在获取流的时候，只能够按照顺序获取，且服务端每次只会处理一个流请求，因此请求和响应都是通过队列来管理的，以FIFO模式的保证顺序。
    另外 stream 方法最终构造的消息对象类型是 StreamRequest。
     */
    // 记录开始处理时间
    final long startTime = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
    }

    // Need to synchronize here so that the callback is added to the queue and the RPC is
    // written to the socket atomically, so that callbacks are called in the right order
    // when responses arrive.
    // 需要在此处进行同步，以便将回调添加到队列中，并且将 RPC 自动地写入套接字，以便在响应到达时以正确的顺序调用回调。
    synchronized (this) {
      // 向 TransportResponseHandler 的 streamCallbacks 队列中存放回调对象，streamCallbacks 是一个 ConcurrentLinkedQueue
      handler.addStreamCallback(callback);
      // 发送请求并添加监听器
      channel.writeAndFlush(new StreamRequest(streamId)).addListener(
        new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {  // 请求发送成功
              long timeTaken = System.currentTimeMillis() - startTime;
              if (logger.isTraceEnabled()) {
                logger.trace("Sending request for {} to {} took {} ms", streamId,
                  getRemoteAddress(channel), timeTaken);
              }
            } else {  // 请求发送失败
              String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
                getRemoteAddress(channel), future.cause());
              logger.error(errorMsg, future.cause());
              channel.close();
              try {
                // 发送请求出错，执行对应的回调方法
                callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
              } catch (Exception e) {
                logger.error("Uncaught exception in RPC response callback handler!", e);
              }
            }
          }
        });
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. The callback will be invoked
   * with the server's response or upon any failure.
   * 向服务端发送 RPC 请求，通过 At least Once Delivery 原则保证请求不会丢失
   *
   * 用于发送 RpcRequest 消息。
   * RpcRequest 消息是需要服务端进行响应的消息，客户端在收到服务端的响应时需要根据情况作出具体的操作；
   * sendRpc 方法（即本方法）的第一个参数是要发送的消息数据，第二个参数则是包装了收到响应后需要进行的操作的回调对象。
   * sendRpc 方法（即本方法）会为每个 RpcRequest 设定一个 requestId（即 return 的 RPC's id），然后将 requestId 和回调对象进行关联，
   * 存放到 TransportResponseHandler 的 outstandingRpcs 字典中，而服务端在对应的响应消息中也会携带相同的 requestId，
   * 这样一来，客户端在收到响应之后，可以根据 requestId 查找当时存放在 TransportResponseHandler 的 outstandingRpcs 字典中的回调对象，
   * 使用该回调对象进行响应处理。
   * 这种设计方法在分布式框架中是非常常见的。
   *
   * 值得注意的是，在使用 Channel 写出数据后，会添加一个监听器监听写出情况，
   * 如果写出失败，会从 TransportResponseHandler 的 outstandingRpcs 字典中移除当时存放的回调对象，
   * 直接调用回调对象的 onFailure 方法以告知失败情况
   *
   * @param message The message to send. 消息.
   * @param callback Callback to handle the RPC's reply. 响应回调处理器.
   * @return The RPC's id.
   */
  public long sendRpc(ByteBuffer message, final RpcResponseCallback callback) {
    // 记录开始时间
    final long startTime = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("Sending RPC to {}", getRemoteAddress(channel));
    }

    // 使用 UUID 生产请求主键
    final long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
    /*
     * 注意这里的操作，向 TransportResponseHandler 添加 requestId 和 RpcResponseCallback 的引用关系。
     * 该 TransportResponseHandler 是在向 Bootstrap 的处理器链中添加 TransportChannelHandler 时添加的，
     * 这一步操作会将 requestId 和回调进行关联，在客户端收到服务端的响应消息时，响应消息中是携带了相同的 requestId 的，
     * 此时就可以通过 requestId 从 TransportResponseHandler 中获取当时发送请求时设置的回调，
     * 达到通过响应结果处理回调的效果。
     */
    handler.addRpcRequest(requestId, callback);

    // 发送 RPC 请求，将消息封装成 RpcRequest 对象，并也是通过通信通道 Channel 对象的 writeAndFlush 方法进行写出
    channel.writeAndFlush(new RpcRequest(requestId, new NioManagedBuffer(message))).addListener(
      new ChannelFutureListener() {  // 添加了汇报发送情况的回调监听器
        // 发送成功会调用该方法
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
          if (future.isSuccess()) {  // 发送成功
            // 时间记录日志
            long timeTaken = System.currentTimeMillis() - startTime;
            if (logger.isTraceEnabled()) {
              logger.trace("Sending request {} to {} took {} ms", requestId,
                getRemoteAddress(channel), timeTaken);
            }
          } else {  // 发送失败
            String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
              getRemoteAddress(channel), future.cause());
            logger.error(errorMsg, future.cause());
            // 从 TransportResponseHandler 中移除 requestId 对应的 RpcResponseCallback
            handler.removeRpcRequest(requestId);
            // 关闭 Channel
            channel.close();
            try {
              // 会调用回调的 onFailure() 方法以告知失败情况
              callback.onFailure(new IOException(errorMsg, future.cause()));
            } catch (Exception e) {
              logger.error("Uncaught exception in RPC response callback handler!", e);
            }
          }
        }
      });

    // 返回 requestId
    return requestId;
  }

  /**
   * Synchronously sends an opaque message to the RpcHandler on the server-side, waiting for up to
   * a specified timeout for a response.
   * 向服务器发送同步的 RPC 请求，并根据指定的超时时间等待响应
   *
   * 用于发送 RpcRequest 消息
   */
  public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
    // 构造用于获取结果的 Future 对象
    final SettableFuture<ByteBuffer> result = SettableFuture.create();

    // 调用 sendRpc() 方法进行发送，通过 SettableFuture 对象获取响应
    sendRpc(message, new RpcResponseCallback() {
      @Override
      public void onSuccess(ByteBuffer response) {
        ByteBuffer copy = ByteBuffer.allocate(response.remaining());
        copy.put(response);
        // flip "copy" to make it readable
        copy.flip();
        // 将响应结果设置到 SettableFuture 对象中
        result.set(copy);
      }

      @Override
      public void onFailure(Throwable e) {
        // 将异常设置到 SettableFuture 对象中
        result.setException(e);
      }
    });

    try {
      // 通过 SettableFuture 对象获取结果，该方法会阻塞，以达到同步效果
      return result.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Sends an opaque message to the RpcHandler on the server-side. No reply is expected for the
   * message, and no delivery guarantees are made.
   * 向服务端发送 RPC 的请求，但不期望能获取响应，因而不能保证投递的可靠性
   *
   * 用于发送不需要回复的 OneWayMessage 消息。
   *
   * @param message The message to send.
   */
  public void send(ByteBuffer message) {
    channel.writeAndFlush(new OneWayMessage(new NioManagedBuffer(message)));
  }

  /**
   * Removes any state associated with the given RPC.
   *
   * @param requestId The RPC id returned by {@link #sendRpc(ByteBuffer, RpcResponseCallback)}.
   */
  public void removeRpcRequest(long requestId) {
    handler.removeRpcRequest(requestId);
  }

  /** Mark this channel as having timed out. */
  public void timeOut() {
    this.timedOut = true;
  }

  @VisibleForTesting
  public TransportResponseHandler getHandler() {
    return handler;
  }

  @Override
  public void close() {
    // close is a local operation and should finish with milliseconds; timeout just to be safe
    channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("remoteAdress", channel.remoteAddress())
      .add("clientId", clientId)
      .add("isActive", isActive())
      .toString();
  }
}
