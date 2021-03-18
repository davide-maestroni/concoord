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
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Iter<T> implements Task<T> {

  private final Iterable<? extends T> messages;

  public Iter(@NotNull T... messages) {
    this(Arrays.asList(messages));
  }

  public Iter(@NotNull final Iterable<? extends T> messages) {
    new IfNull("messages", messages).throwException();
    this.messages = messages;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(scheduler, new IterControl<T>(messages.iterator()));
  }

  private static class IterControl<T> implements ExecutionControl<T> {

    private final Iterator<? extends T> iterator;

    private IterControl(@NotNull Iterator<? extends T> iterator) {
      new IfNull("iterator", iterator).throwException();
      this.iterator = iterator;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
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

    public void abortExecution() {
    }
  }
}
