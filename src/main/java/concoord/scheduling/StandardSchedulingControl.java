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
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.InputChannel;
import concoord.lang.Parallel.OutputChannel;
import concoord.lang.Parallel.SchedulingControl;
import concoord.scheduling.output.OutputControl;
import concoord.scheduling.strategy.SchedulingStrategy;
import concoord.scheduling.streaming.StreamingControl;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import org.jetbrains.annotations.NotNull;

public class StandardSchedulingControl<T, M> implements SchedulingControl<T, M> {

  private final int maxParallelism;
  private final SchedulerFactory schedulerFactory;
  private final SchedulingStrategy<? super M> schedulingStrategy;
  private final StreamingControl<T, M> streamingControl;
  private final OutputControl<T> outputControl;

  public StandardSchedulingControl(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingStrategy<? super M> schedulingStrategy,
      @NotNull StreamingControl<T, M> streamingControl, @NotNull OutputControl<T> outputControl) {
    new IfSomeOf(
        new IfNull("schedulerFactory", schedulerFactory),
        new IfNull("schedulingStrategy", schedulingStrategy),
        new IfNull("streamingControl", streamingControl),
        new IfNull("outputControl", outputControl)
    ).throwException();
    this.maxParallelism = maxParallelism;
    this.schedulerFactory = schedulerFactory;
    if (maxParallelism < 0) {
      // infinite parallelism
      this.schedulingStrategy = new FactoryStrategy<M>();
    } else if (maxParallelism == 0) {
      // no parallelism
      this.schedulingStrategy = new TrampolineFactory<M>();
    } else {
      this.schedulingStrategy = schedulingStrategy;
    }
    this.streamingControl = streamingControl;
    this.outputControl = outputControl;
  }

  @NotNull
  public Awaitable<T> schedule(M message, @NotNull Block<T, ? super M> block) throws Exception {
    final Scheduler scheduler =
        schedulingStrategy.schedulerFor(message, maxParallelism, schedulerFactory);
    return streamingControl.stream(scheduler, message, block);
  }

  public void end() throws Exception {
    streamingControl.end();
  }

  @NotNull
  public InputChannel<T> outputBufferInput() throws Exception {
    return outputControl.outputBufferInput();
  }

  @NotNull
  public OutputChannel<T> outputBufferOutput() throws Exception {
    return outputControl.outputBufferOutput();
  }

  private static class FactoryStrategy<M> implements SchedulingStrategy<M> {

    @NotNull
    public Scheduler schedulerFor(M message, int maxParallelism,
        @NotNull SchedulerFactory schedulerFactory) throws Exception {
      return schedulerFactory.create();
    }
  }

  private static class TrampolineFactory<M> implements SchedulingStrategy<M> {

    private final Trampoline trampoline = new Trampoline();

    @NotNull
    public Scheduler schedulerFor(M message, int maxParallelism,
        @NotNull SchedulerFactory schedulerFactory) {
      return trampoline;
    }
  }
}
