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

import io.netty.channel.Channel;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * The StreamManager is used to fetch individual chunks from a stream. This is used in
 * {@link TransportRequestHandler} in order to respond to fetchChunk() requests. Creation of the
 * stream is outside the scope of the transport layer, but a given stream is guaranteed to be read
 * by only one client connection, meaning that getChunk() for a particular stream will be called
 * serially and that once the connection associated with the stream is closed, that stream will
 * never be used again.
 * StreamManager 用于从流中获取单个块。
 * TransportRequestHandler 中使用了它，以响应 fetchChunk() 请求。
 * 流的创建不在传输层的范围内，但是保证给定的流只能由一个客户端连接读取，这意味着特定流的 getChunk() 将被串行调用，
 * 并且一旦连接关联在关闭流的情况下，该流将不再被使用。
 *
 * StreamManager 主要用于处理 ChunkFetchRequest 和 StreamRequest 两种消息，它们分别代表块获取请求和流请求。
 */
public abstract class StreamManager {
  /**
   * Called in response to a fetchChunk() request. The returned buffer will be passed as-is to the
   * client. A single stream will be associated with a single TCP connection, so this method
   * will not be called in parallel for a particular stream.
   * 在响应 fetchChunk() 请求时调用。
   * 返回的缓冲区将按原样传递给客户端。单个流将与单个 TCP 连接相关联，因此不会为特定流并行调用此方法。
   *
   * Chunks may be requested in any order, and requests may be repeated, but it is not required
   * that implementations support this behavior.
   * 可以按任何顺序请求块，并且可以重复请求，但是不需要实现支持此行为。
   *
   * The returned ManagedBuffer will be release()'d after being written to the network.
   * 返回的 ManagedBuffer 将被写入网络后将被释放。
   *
   * 用于从 Stream ID 指定的流中获取索引从 0 至 chunkIndex 的块数据，返回的是 ManagedBuffer 对象
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @param chunkIndex 0-indexed chunk of the stream that's requested
   */
  public abstract ManagedBuffer getChunk(long streamId, int chunkIndex);

  /**
   * Called in response to a stream() request. The returned data is streamed to the client
   * through a single TCP connection.
   * 在响应 stream() 请求时调用。返回的数据通过单个 TCP 连接流传输到客户端。
   *
   * Note the <code>streamId</code> argument is not related to the similarly named argument in the
   * {@link #getChunk(long, int)} method.
   * 请注意，streamId 参数与 getChunk(long，int) 方法中名称相似的参数无关。
   *
   * 打开 Stream ID 对应的流，返回的是 ManagedBuffer 对象
   *
   * @param streamId id of a stream that has been previously registered with the StreamManager.
   * @return A managed buffer for the stream, or null if the stream was not found.
   */
  public ManagedBuffer openStream(String streamId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Associates a stream with a single client connection, which is guaranteed to be the only reader
   * of the stream. The getChunk() method will be called serially on this connection and once the
   * connection is closed, the stream will never be used again, enabling cleanup.
   * 将流与单个客户端连接关联，该客户端连接保证是该流的唯一读取器。
   * 将在此连接上依次调用 getChunk() 方法，并且连接关闭后，将不再使用该流，从而启用清除功能。
   *
   * This must be called before the first getChunk() on the stream, but it may be invoked multiple
   * times with the same channel and stream id.
   * 必须在流的第一个 getChunk() 之前调用此方法，但是可以使用相同的通道和流ID多次调用它
   *
   * 将指定的 Channel 与流绑定
   */
  public void registerChannel(Channel channel, long streamId) { }

  /**
   * Indicates that the given channel has been terminated. After this occurs, we are guaranteed not
   * to read from the associated streams again, so any state can be cleaned up.
   * 指示给定的频道已被终止。发生这种情况后，我们保证不会再从关联的流中读取数据，因此可以清除任何状态。
   *
   * 关闭 Channel。该操作执行后 Channel 绑定的流将不会被读取，Channel 与流的绑定也会被清除
   */
  public void connectionTerminated(Channel channel) { }

  /**
   * Verify that the client is authorized to read from the given stream.
   * 验证客户端是否有权从给定流中读取。
   *
   * @throws SecurityException If client is not authorized.
   */
  public void checkAuthorization(TransportClient client, long streamId) { }

}
