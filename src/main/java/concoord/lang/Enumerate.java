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
import concoord.flow.Result;
import concoord.lang.For.Block;
import concoord.tuple.Binary;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import org.jetbrains.annotations.NotNull;

public class Enumerate<T, M> implements Task<T> {

  private final For<T, M> task;

  public Enumerate(@NotNull final Awaitable<M> awaitable,
      @NotNull Block<T, ? super Binary<? super Integer, ? super M>> block) {
    this(1, awaitable, block);
  }

  public Enumerate(int maxEvents, @NotNull final Awaitable<M> awaitable,
      @NotNull Block<T, ? super Binary<? super Integer, ? super M>> block) {
    new IfSomeOf(
        new IfNull(awaitable, "awaitable"),
        new IfNull(block, "block")
    ).throwException();
    this.task = new For<T, M>(maxEvents, awaitable, new EnumerateBlock<T, M>(block));
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return task.on(scheduler);
  }

  private static class EnumerateBlock<T, M> implements Block<T, M> {

    private final Block<T, ? super Binary<? super Integer, ? super M>> block;
    private int index;

    private EnumerateBlock(@NotNull Block<T, ? super Binary<? super Integer, ? super M>> block) {
      this.block = block;
    }

    @NotNull
    public Result<T> execute(M message) throws Exception {
      return block.execute(new Binary<Integer, M>(index++, message));
    }
  }
}
