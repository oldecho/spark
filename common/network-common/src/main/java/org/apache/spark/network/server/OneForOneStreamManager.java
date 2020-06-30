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

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClient;

/**
 * StreamManager which allows registration of an Iterator&lt;ManagedBuffer&gt;, which are
 * individually fetched as chunks by the client. Each registered buffer is one chunk.
 *
 * 允许注册 Iterator<ManagedBuffer> 的 StreamManager，由客户端分别作为块分别提取。每个注册缓冲区是一个块。
 * OneForOneStreamManager 实现了 StreamManager 中除 openStream() 以外所有的方法。
 */
public class OneForOneStreamManager extends StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(OneForOneStreamManager.class);

  /** 用于生成数据流的标识，类型为 AtomicLong. */
  private final AtomicLong nextStreamId;
  /** 维护 streamId 与 StreamState 之间映射关系的缓存. */
  private final ConcurrentHashMap<Long, StreamState> streams;

  /**
   * State of a single stream.
   * 这里我们只需要关注，每个流只会所属于一个 Application，同时流的表示其实是一个 Iterator 类型的迭代器对象。
   */
  private static class StreamState {
    // 请求流所属的应用程序ID。此属性只有在ExternalShuffleClient启用后才会用到。
    final String appId;
    // ManagedBuffer 的缓冲。
    final Iterator<ManagedBuffer> buffers;

    // The channel associated to the stream
    // 与当前流相关联的 Channel。
    Channel associatedChannel = null;

    // Used to keep track of the index of the buffer that the user has retrieved, just to ensure
    // that the caller only requests each chunk one at a time, in order.
    // 为了保证客户端按顺序每次请求一个块，所以用此属性跟踪客户端当前接收到的 ManagedBuffer 的索引。
    int curChunk = 0;

    StreamState(String appId, Iterator<ManagedBuffer> buffers) {
      this.appId = appId;
      this.buffers = Preconditions.checkNotNull(buffers);
    }
  }

  public OneForOneStreamManager() {
    // For debugging purposes, start with a random stream id to help identifying different streams.
    // This does not need to be globally unique, only unique to this class.
    // 为了进行调试，请从随机流ID开始，以帮助识别不同的流。
    // 这不必是全局唯一的，而仅是此类的唯一。
    nextStreamId = new AtomicLong((long) new Random().nextInt(Integer.MAX_VALUE) * 1000);
    streams = new ConcurrentHashMap<>();
  }

  /**
   * 注册 Channel。将一个流和一条（只能是一条）客户端的TCP连接关联起来，
   * 这可以保证对于单个的流只会有一个客户端读取。流关闭之后就永远不能够重用了。
   */
  @Override
  public void registerChannel(Channel channel, long streamId) {
    if (streams.containsKey(streamId)) {
      // 将传入 Channel 关联到传入的 streamId 对应的 StreamState 的 associatedChannel 字段上
      streams.get(streamId).associatedChannel = channel;
    }
  }

  /**
   * 获取单个的块（块被封装为 ManagedBuffer）
   * 从该方法的实现可知，所谓的获取块，其实就是迭代获取流对应的 Iterator 类型的迭代器 buffers 中的元素并返回。
   * 这里需要注意的一点是，StreamState 的 curChunk 记录了下次将要拉取的块索引，在每次获取块时都会校验该索引，以保证拉取操作是按序进行的。
   */
  @Override
  public ManagedBuffer getChunk(long streamId, int chunkIndex) {
    // 从 streams 中获取 StreamState
    StreamState state = streams.get(streamId);
    if (chunkIndex != state.curChunk) {
      // 获取的块不等于当前块，抛出异常
      throw new IllegalStateException(String.format(
        "Received out-of-order chunk index %s (expected %s)", chunkIndex, state.curChunk));
    } else if (!state.buffers.hasNext()) {
      // buffers 缓冲中的 ManagedBuffer 已经全部被客户端获取，抛出异常
      throw new IllegalStateException(String.format(
        "Requested chunk index beyond end %s", chunkIndex));
    }
    // 将 StreamState 的 curChunk 加 1，为下次接收请求做好准备
    state.curChunk += 1;
    // 从 buffers 缓冲中获取 ManagedBuffer
    ManagedBuffer nextChunk = state.buffers.next();

    if (!state.buffers.hasNext()) {
      // buffers 缓冲中的 ManagedBuffer 已经全部被客户端获取，移除对应的 StreamState
      logger.trace("Removing stream id {}", streamId);
      streams.remove(streamId);
    }

    return nextChunk;
  }

  /** 取消 Channel 与流的绑定. */
  @Override
  public void connectionTerminated(Channel channel) {
    // Close all streams which have been associated with the channel.
    for (Map.Entry<Long, StreamState> entry: streams.entrySet()) {
      StreamState state = entry.getValue();
      if (state.associatedChannel == channel) {
        streams.remove(entry.getKey());

        // Release all remaining buffers.
        while (state.buffers.hasNext()) {
          state.buffers.next().release();
        }
      }
    }
  }

  /**
   * 实现比较简单，其实就是校验 TransportClient 中保存的 Client ID 是否与 streamId 指定的流的 appId 相同，如果不同则不允许访问该流。
   */
  @Override
  public void checkAuthorization(TransportClient client, long streamId) {
    // 如果没有配置对管道进行SASL认证，TransportClient 的 clientId 为 null，因而实际上并不走权限检查。
    // 当启用了SASL认证，客户端需要给 TransportClient 的 clientId 赋值，因此才会走此检查。
    if (client.getClientId() != null) {
      // 获取 streamId 对应的流状态
      StreamState state = streams.get(streamId);
      // 检查对应的 StreamState 是否为空
      Preconditions.checkArgument(state != null, "Unknown stream ID.");
      // TransportClient 的 clientId 属性值是否与 streamId 对应的 StreamState 的 appId 的值相等
      if (!client.getClientId().equals(state.appId)) {
        // 不相等说明权限验证失败
        throw new SecurityException(String.format(
          "Client %s not authorized to read stream %d (app %s).",
          client.getClientId(),
          streamId,
          state.appId));
      }
    }
  }

  /**
   * Registers a stream of ManagedBuffers which are served as individual chunks one at a time to
   * callers. Each ManagedBuffer will be release()'d after it is transferred on the wire. If a
   * client connection is closed before the iterator is fully drained, then the remaining buffers
   * will all be release()'d.
   * 注册一个 ManagedBuffers 流，一次作为单个块提供给调用者。
   * 每个 ManagedBuffer 在网络上传输后都将被释放。
   * 如果客户端连接在迭代器完全耗尽之前关闭，则其余的缓冲区将全部由 release() 释放。
   *
   * If an app ID is provided, only callers who've authenticated with the given app ID will be
   * allowed to fetch from this stream.
   * 如果提供了 App ID，则仅已通过给定 App ID 进行身份验证的呼叫者将被允许从此流中获取。
   *
   * 向 OneForOneStreamManager 的 streams 缓存中注册流。
   * 所谓的注册，其实就是使用 nextStreamId 分配一个 Stream ID，
   * 然后根据传入的 appId 和 buffers 创建一个 StreamState 对象，存入 streams 字典中即可。
   */
  public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
    // 生成新的 Stream ID
    long myStreamId = nextStreamId.getAndIncrement();
    // 添加到 streams 字典
    streams.put(myStreamId, new StreamState(appId, buffers));
    // 返回生成的 Stream ID
    return myStreamId;
  }

}
