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
import concoord.concurrent.SchedulerFactory;
import concoord.data.Buffer;
import concoord.data.Buffered;
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.InputChannel;
import concoord.lang.Parallel.OutputChannel;
import concoord.lang.Parallel.SchedulingControl;
import concoord.scheduling.output.UnorderedOutput;
import concoord.scheduling.strategy.SchedulingStrategy;
import concoord.scheduling.streaming.PartialStreaming;
import concoord.scheduling.streaming.SingleStreaming;
import concoord.scheduling.streaming.StreamingControl;
import org.jetbrains.annotations.NotNull;

public class Unordered<T, M> implements SchedulingControl<T, M> {

  private final StandardSchedulingControl<T, M> control;

  public Unordered(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingStrategy<? super M> schedulingStrategy) {
    this(maxParallelism, schedulerFactory, schedulingStrategy, new Buffered<T>());
  }

  public Unordered(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingStrategy<? super M> schedulingStrategy, int initialCapacity) {
    this(maxParallelism, schedulerFactory, schedulingStrategy, new Buffered<T>(initialCapacity));
  }

  public Unordered(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingStrategy<? super M> schedulingStrategy, @NotNull Buffer<T> buffer) {
    final StreamingControl<T, M> streamingControl;
    if (maxParallelism < 0) {
      // infinite parallelism
      streamingControl = new SingleStreaming<T, M>();
    } else {
      streamingControl = new PartialStreaming<T, M>();
    }
    this.control = new StandardSchedulingControl<T, M>(
        maxParallelism,
        schedulerFactory,
        schedulingStrategy,
        streamingControl,
        new UnorderedOutput<T>(buffer)
    );
  }

  @NotNull
  public Awaitable<T> schedule(M message, @NotNull Block<T, ? super M> block) throws Exception {
    return control.schedule(message, block);
  }

  public void end() throws Exception {
    control.end();
  }

  @NotNull
  public InputChannel<T> outputBufferInput() throws Exception {
    return control.outputBufferInput();
  }

  @NotNull
  public OutputChannel<T> outputBufferOutput() throws Exception {
    return control.outputBufferOutput();
  }
}
