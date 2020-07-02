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

package org.apache.spark.scheduler

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.DynamicVariable

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 *
 * 采用异步线程将 SparkListenerEvent 类型的事件投递到 SparkListener 类型的监听器
 * 达到实时刷新UI洁面数据的效果
 */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {

  self =>

  import LiveListenerBus._

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  // eventQueue 是 SparkListenerEvent 事件的阻塞队列
  // 队列大小可以通过 Spark 属性 spark.scheduler.listenerbus.eventqueue.size 进行配置，默认 10000
  private lazy val EVENT_QUEUE_CAPACITY = validateAndGetQueueSize()
  private lazy val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)

  private def validateAndGetQueueSize(): Int = {
    // 默认 10000
    val queueSize = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_SIZE)
    if (queueSize <= 0) {
      throw new SparkException("spark.scheduler.listenerbus.eventqueue.size must be > 0!")
    }
    queueSize
  }

  // Indicate if `start()` is called，标记 LiveListenerBus 的启动状态的 AtomicBoolean 类型的变量
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called，标记 LiveListenerBus 的停止状态的 AtomicBoolean 类型的变量
  private val stopped = new AtomicBoolean(false)

  /**
   * A counter for dropped events. It will be reset every time we log it.
   * 丢弃事件的计数器。每次我们记录它都会被重置。
   * 使用 AtomicLong 类型对删除的事件进行技术，每当日志打印了 droppedEventsCounter 后，会将 droppedEventsCounter 重置为 0
   */
  private val droppedEventsCounter = new AtomicLong(0L)

  /**
   * When `droppedEventsCounter` was logged last time in milliseconds.
   * 用于记录最后一次日志打印 droppedEventsCounter 的时间戳
   */
  @volatile private var lastReportTimestamp = 0L

  // Indicate if we are processing some event
  // Guarded by `self`
  // 用来标记当前正有事件被 listenerThread 线程处理
  private var processingEvent = false

  // AtomicBoolean 类型的变量，用于标记是否由于 eventQueue 已满，导致新的事件被删除
  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  // 用于当有新的事件到来时释放信号量，当对事件进行处理时获取信号量
  private val eventLock = new Semaphore(0)

  /**
   * 处理事件的线程
   * - 不断获取信号量（当可以获取信号量时，说明你还有事件未处理）
   * - 通过同步控制。将 processingEvent 设置为 true
   * - 从 eventQueue 中获取事件
   * - 调用超类 ListenerBus 的 postToAll 方法，postToAll 对监听器进行遍历，并调用 SparkListenerBus 的 doPostEvent 方法对事件进行匹配后执行监听器的相应方法
   * - 每次循环结束依然需要通过同步控制，将 processingEvent 设置为 false
   *
   * run 方法中使用了 Utils.tryOrStopSparkContext 方法，可以保证当 listenerThread 的内部循环跑出一场后
   * 启动一个新的线程停止 SparkContext
   */
  private val listenerThread = new Thread(name) {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          // 获取信号量
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            // 从 eventQueue 中获取事件
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              // 退出 while 循环并关闭守护程序线程
              if (!stopped.get) {
                // 获取到了信号量却 poll 到了 null，说明这个信号量来自 stop()
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            // 事件处理
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   * 开始向附加的侦听器发送事件。
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   * 它首先发出在此侦听器总线启动之前发布的所有缓冲事件，然后在侦听器总线仍在运行时异步侦听任何其他事件。 仅应调用一次。
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  /**
   * 向 eventQueue 队列添加事件
   * eventQueue 是 LiveListenerBus 的属性，ListenerBus 和 SparkListenerBus 并没有操作 eventQueue 的方法
   *
   * - 判断 LiveListenerBus 是否处于停止状态
   * - 向 eventQueue 中添加时间。
   *   如果添加成功，则释放信号量，进而催化 listenerThread 有效工作。
   *   如果 eventQueue 已满造成失败，则移除事件，并对删除事件计数器 droppedEventsCounter 进行自增。
   * - 如果有事件被删除，并且当前系统时间距离上一次打印 droppedEventsCounter 超过了 60s，则将 droppedEventsCounter 答应到日志上。
   */
  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      // 丢弃其他事件以使 `listenerThread` 尽快退出
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    // offer 添加成功返回 true，否则 false（LinkedBlockingQueue 容量不足时，add抛异常，put 阻塞等待，offer 立即返回是否添加成功）
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      onDropEvent(event)
      droppedEventsCounter.incrementAndGet()
    }

    val droppedEvents = droppedEventsCounter.get
    if (droppedEvents > 0) {
      // Don't log too frequently
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        // 可能有多个线程试图减少dropEventsCounter。
        // 使用 compareAndSet 确保只有一个线程可以操作。
        // 如果另一个线程正在增加 dropEventsCounter，则 compareAndSet 将失败，然后该线程将对其进行更新。
        if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
            new java.util.Date(prevLastReportTimestamp))
        }
      }
    }
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  /**
   * For testing only. Return whether the listener daemon thread is still alive.
   * Exposed for testing.
   */
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
   * Return whether the event queue is empty.
   *
   * The use of synchronized here guarantees that all events that once belonged to this queue
   * have already been processed by all attached listeners, if this returns true.
   */
  private def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty && !processingEvent }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      // 调用 eventLock.release()，以便 listenerThread 将从 eventQueue 中轮询到 null 从而知道 stop() 已经被调用。
      eventLock.release()
      listenerThread.join()
    } else {
      // Keep quiet
    }
  }

  /**
   * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
   * notified with the dropped events.
   * 如果事件队列超出其容量，则将删除新事件。 子类将通过丢弃事件进行通知。
   *
   * Note: `onDropEvent` can be called in any thread.
   */
  def onDropEvent(event: SparkListenerEvent): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }
}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** The thread name of Spark listener bus */
  val name = "SparkListenerBus"
}

