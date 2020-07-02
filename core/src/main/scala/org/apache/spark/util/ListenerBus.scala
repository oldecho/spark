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

package org.apache.spark.util

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * An event bus which posts events to its listeners.
 * 事件总线，将事件发布到其监听器。
 * L代表监听器的泛型参数，表示支持任何类型的监听器
 * E代表事件的泛型参数
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  /**
   * Marked `private[spark]` for access in tests.
   * 用户维护所有注册的监听器
   */
  private[spark] val listeners = new CopyOnWriteArrayList[L]

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   * 向 listeners 添加监听器。
   * 因为 listeners 采用 CopyOnWriteArrayList 来实现，所以此方法是线程安全的。
   */
  final def addListener(listener: L): Unit = {
    listeners.add(listener)
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
   * 从 listeners 中移除一个监听器。
   * 因为 listeners 采用 CopyOnWriteArrayList 来实现，所以此方法是线程安全的。
   */
  final def removeListener(listener: L): Unit = {
    listeners.remove(listener)
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   * 将事件发布到所有注册的监听器。
   * 虽然 listeners 是线程安全的，但是 `postToAll` 引入了"先检查后执行"的逻辑，因此本方法不是线程安全的，
   * 所以所有对 `postToAll` 方法的调用应当保证在同一个线程中。
   */
  final def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    // 如果我们使用 asScala，则 JavaConverters 可以创建 JIterableWrapper。
    // 但是，此方法将被频繁调用。为了避免包装成本，这里我们直接使用 Java Iterator
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
   * 将事件发布到指定的监听器。确保对所有监听器在同一线程中调用 `onPostEvent`。
   * 需子类实现。
   */
  protected def doPostEvent(listener: L, event: E): Unit

  /** 查找与指定类型相同的监听器列表. */
  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}
