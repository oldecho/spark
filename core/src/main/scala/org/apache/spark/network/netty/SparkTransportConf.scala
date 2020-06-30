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

package org.apache.spark.network.netty

import org.apache.spark.SparkConf
import org.apache.spark.network.util.{ConfigProvider, TransportConf}

/**
 * Provides a utility for transforming from a SparkConf inside a Spark JVM (e.g., Executor,
 * Driver, or a standalone shuffle service) into a TransportConf with details on our environment
 * like the number of cores that are allocated to this JVM.
 *
 * 提供一个工具，将 Spark JVM 内部的 SparkConf（例如，Executor，Driver 或独立的 shuffle 服务）转换为 TransportConf，
 * 其中包含有关我们环境的详细信息，例如分配给此 JVM 的内核数。
 */
object SparkTransportConf {
  /**
   * Specifies an upper bound on the number of Netty threads that Spark requires by default.
   * In practice, only 2-4 cores should be required to transfer roughly 10 Gb/s, and each core
   * that we use will have an initial overhead of roughly 32 MB of off-heap memory, which comes
   * at a premium.
   * 指定默认情况下 Spark 需要的 Netty 线程数的上限。
   * 实际上，只需要 2-4 个内核即可传输大约 10 Gb/s，并且我们使用的每个内核的初始开​​销大约为 32 MB 的堆外内存，而这是非常宝贵的。
   *
   * Thus, this value should still retain maximum throughput and reduce wasted off-heap memory
   * allocation. It can be overridden by setting the number of serverThreads and clientThreads
   * manually in Spark's configuration.
   * 因此，该值仍应保留最大吞吐量，并减少浪费的堆外内存分配。
   * 可以通过在 Spark 的配置中手动设置 serverThreads 和 clientThreads 的数量来覆盖它。
   */
  private val MAX_DEFAULT_NETTY_THREADS = 8

  /**
   * Utility for creating a [[TransportConf]] from a [[SparkConf]].
   * @param _conf the [[SparkConf]]
   * @param module the module name
   * @param numUsableCores if nonzero, this will restrict the server and client threads to only
   *                       use the given number of cores, rather than all of the machine's cores.
   *                       This restriction will only occur if these properties are not already set.
   *                       如果非零，这将限制服务器和客户端线程仅使用给定数量的内核，而不使用所有计算机内核。
   *                       仅当尚未设置这些属性时，才会发生此限制。
   */
  def fromSparkConf(_conf: SparkConf, module: String, numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone

    // Specify thread configuration based on our JVM's allocation of cores (rather than necessarily
    // assuming we have all the machine's cores).
    // NB: Only set if serverThreads/clientThreads not already set.
    // 根据我们的 JVM 内核分配（而不是我们拥有所有机器内核）来指定线程配置。
    // 仅在尚未设置 serverThreads / clientThreads 时设置。
    val numThreads = defaultNumThreads(numUsableCores)
    conf.setIfMissing(s"spark.$module.io.serverThreads", numThreads.toString)
    conf.setIfMissing(s"spark.$module.io.clientThreads", numThreads.toString)

    new TransportConf(module, new ConfigProvider {
      // 通过创建 ConfigProvider 匿名内部类，实现 get 方法，代理了 SparkConf 的 get 方法
      override def get(name: String): String = conf.get(name)
    })
  }

  /**
   * Returns the default number of threads for both the Netty client and server thread pools.
   * If numUsableCores is 0, we will use Runtime get an approximate number of available cores.
   *
   * numUsableCores <= 0 时取运行时可用核数，否则取本身
   * 同时限制最大线程数 8
   */
  private def defaultNumThreads(numUsableCores: Int): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    math.min(availableCores, MAX_DEFAULT_NETTY_THREADS)
  }
}
