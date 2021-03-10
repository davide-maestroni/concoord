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
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Or<T> implements Task<T> {

  private final Iterable<? extends Awaitable<? extends T>> awaitables;

  public Or(@NotNull Awaitable<? extends T>... awaitables) {
    this(Arrays.asList(awaitables));
  }

  public Or(@NotNull Iterable<? extends Awaitable<? extends T>> awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", awaitables)
    ).throwException();
    this.awaitables = awaitables;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull("scheduler", scheduler).throwException();
    return new OrAwaitable<T>(scheduler, awaitables.iterator());
  }

  private static class OrAwaitable<T> extends BaseAwaitable<T> {

    private final Iterator<? extends Awaitable<? extends T>> iterator;

    public OrAwaitable(@NotNull Scheduler scheduler,
        @NotNull Iterator<? extends Awaitable<? extends T>> iterator) {
      super(scheduler);
      new IfNull("iterator", iterator).throwException();
      this.iterator = iterator;
    }

    @Override
    protected boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
      if (!flowControl.hasOutputs() && iterator.hasNext()) {
        flowControl.postOutput(iterator.next());
      } else {
        flowControl.stop();
      }
      return true;
    }

    @Override
    protected void cancelExecution(@NotNull AwaitableFlowControl<T> flowControl) {
    }

    @Override
    protected void abortExecution() {
    }
  }
}
