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
import concoord.lang.Parallel.SchedulingControl;
import concoord.lang.Streamed;
import concoord.lang.Streamed.StreamedAwaitable;
import concoord.scheduling.strategy.SchedulingStrategy;
import org.jetbrains.annotations.NotNull;

public class Each<T, M> implements SchedulingControl<T, M> {

  private final SchedulingStrategy<? super M> strategy;
  private final Block<T, M> block;

  public Each(@NotNull SchedulingStrategy<? super M> strategy, @NotNull Block<T, M> block) {
    this.strategy = strategy;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    final Scheduler scheduler = strategy.schedulerFor(message);
    final StreamedAwaitable<M> input = new Streamed<M>(new ConsumingFactory<M>())
        .on(new Trampoline());
    final Awaitable<T> output = block.execute(scheduler, input);
    input.message(message);
    input.end();
    return output;
  }

  public void stopAll() {
  }
}
