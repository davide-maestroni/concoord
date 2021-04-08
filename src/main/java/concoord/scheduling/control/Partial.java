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
package concoord.scheduling.control;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.concurrent.Trampoline;
import concoord.data.ConsumingFactory;
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.lang.Streamed;
import concoord.lang.Streamed.StreamedAwaitable;
import concoord.scheduling.strategy.StandardSchedulingStrategy.SchedulingControl;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class Partial<T, M> implements SchedulingStrategy<T, M> {

  private final WeakHashMap<Scheduler, ScheduledTask<T, M>> tasks =
      new WeakHashMap<Scheduler, ScheduledTask<T, M>>();
  private final SchedulingControl<M> schedulingControl;
  private final Block<T, M> block;

  public Partial(@NotNull SchedulingControl<M> schedulingControl, @NotNull Block<T, M> block) {
    this.schedulingControl = schedulingControl;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    final Scheduler scheduler = schedulingControl.schedulerFor(message);
    ScheduledTask<T, M> task = tasks.get(scheduler);
    if (task == null) {
      final StreamedAwaitable<M> input = new Streamed<M>(new ConsumingFactory<M>())
          .on(new Trampoline());
      final Awaitable<T> output = block.execute(input, scheduler);
      task = new ScheduledTask<T, M>(input, output);
      tasks.put(scheduler, task);
    }
    task.input().message(message);
    return task.output();
  }

  public void stopAll() throws Exception {
    for (ScheduledTask<T, M> task : tasks.values()) {
      task.input().end();
    }
  }

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
}
