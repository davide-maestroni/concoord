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
import org.jetbrains.annotations.NotNull;

public class Each<T, M> implements SchedulingStrategy<T, M> {

  private final SchedulingControl<M> schedulingControl;
  private final Block<T, M> block;

  public Each(@NotNull SchedulingControl<M> schedulingControl, @NotNull Block<T, M> block) {
    this.schedulingControl = schedulingControl;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    final Scheduler scheduler = schedulingControl.schedulerFor(message);
    final StreamedAwaitable<M> input = new Streamed<M>(new ConsumingFactory<M>())
        .on(new Trampoline());
    final Awaitable<T> output = block.execute(input, scheduler);
    input.message(message);
    input.end();
    return output;
  }

  public void stopAll() {
  }
}
