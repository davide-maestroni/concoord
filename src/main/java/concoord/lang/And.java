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
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class And<T> implements Task<T> {

  private final Iterable<? extends Awaitable<? extends T>> awaitables;

  public And(@NotNull Awaitable<? extends T>... awaitables) {
    this(Arrays.asList(awaitables));
  }

  public And(@NotNull final Iterable<? extends Awaitable<? extends T>> awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", awaitables)
    ).throwException();
    this.awaitables = awaitables;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(scheduler, new AndControl<T>(awaitables.iterator()));
  }

  private static class AndControl<T> implements ExecutionControl<T> {

    private final Iterator<? extends Awaitable<? extends T>> iterator;

    public AndControl(@NotNull Iterator<? extends Awaitable<? extends T>> iterator) {
      new IfNull("iterator", iterator).throwException();
      this.iterator = iterator;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
      if (iterator.hasNext()) {
        flowControl.postOutput(iterator.next());
      } else {
        flowControl.stop();
      }
      return true;
    }

    public void cancelExecution() {
    }

    public void abortExecution() {
    }
  }
}
