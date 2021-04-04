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
package concoord.scheduling;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.concurrent.SchedulerFactory;
import concoord.concurrent.Trampoline;
import concoord.data.Buffer;
import concoord.data.BufferFactory;
import concoord.data.Consuming;
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.lang.Streamed;
import concoord.lang.Streamed.StreamedAwaitable;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

abstract class AbstractSchedulingStrategy<T, M> implements SchedulingStrategy<T, M> {

  private final WeakHashMap<Scheduler, ScheduledTask<T, M>> tasks =
      new WeakHashMap<Scheduler, ScheduledTask<T, M>>();
  private final SchedulerFactory factory;
  private final Block<T, M> block;

  AbstractSchedulingStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull Block<T, M> block) {
    new IfSomeOf(
        new IfNull("schedulerFactory", schedulerFactory),
        new IfNull("block", block)
    ).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.factory = new TrampolineFactory();
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.factory = schedulerFactory;
    } else {
      this.factory = create(maxParallelism, schedulerFactory);
    }
    this.block = block;
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    final Scheduler scheduler = factory.create();
    ScheduledTask<T, M> task = tasks.get(scheduler);
    if (task == null) {
      final StreamedAwaitable<M> input = new Streamed<M>(new ConsumingFactory<M>())
          .on(new Trampoline());
      final Awaitable<T> output = block.execute(scheduler, input);
      task = new ScheduledTask<T, M>(input, output);
      tasks.put(scheduler, task);
    }
    task.input().message(message);
    return task.output();
  }

  @NotNull
  abstract SchedulerFactory create(int maxParallelism, @NotNull SchedulerFactory schedulerFactory);

  private static class ScheduledTask<T, M> {

    private final StreamedAwaitable<M> input;
    private final Awaitable<T> output;

    private ScheduledTask(@NotNull StreamedAwaitable<M> input, @NotNull Awaitable<T> output) {
      this.input = input;
      this.output = output;
    }

    @NotNull
    private StreamedAwaitable<M> input() {
      return input;
    }

    @NotNull
    private Awaitable<T> output() {
      return output;
    }
  }

  private static class ConsumingFactory<M> implements BufferFactory<M> {

    @NotNull
    public Buffer<M> create() {
      return new Consuming<M>();
    }
  }
}
