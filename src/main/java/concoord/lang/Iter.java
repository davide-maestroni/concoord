/*
 * Copyright 2021 Davide Maestroni
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package concoord.lang;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Iter<T> implements Task<T> {

  private final Task<T> task;

  public Iter(@NotNull T... messages) {
    this(Arrays.asList(messages));
  }

  public Iter(@NotNull final Iterable<? extends T> messages) {
    new IfNull(messages, "messages").throwException();
    this.task = new Task<T>() {
      @NotNull
      public Awaitable<T> on(@NotNull Scheduler scheduler) {
        return new IterAwaitable<T>(scheduler, messages.iterator());
      }
    };
  }

  public Iter(@NotNull final Iterator<? extends T> messages) {
    new IfNull(messages, "messages").throwException();
    this.task = new Task<T>() {
      @NotNull
      public Awaitable<T> on(@NotNull Scheduler scheduler) {
        return new IterAwaitable<T>(scheduler, messages);
      }
    };
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return task.on(scheduler);
  }

  private static class IterAwaitable<T> extends BaseAwaitable<T> {

    private final Iterator<? extends T> iterator;

    private IterAwaitable(@NotNull Scheduler scheduler, @NotNull Iterator<? extends T> iterator) {
      super(scheduler);
      this.iterator = iterator;
    }

    protected boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
      flowControl.logger().log(
          new DbgMessage("[executing] next iteration: %s", new PrintIdentity(iterator))
      );
      if (iterator.hasNext()) {
        flowControl.postOutput(iterator.next());
      } else {
        flowControl.stop();
      }
      return true;
    }

    protected void cancelExecution() {
    }
  }
}
