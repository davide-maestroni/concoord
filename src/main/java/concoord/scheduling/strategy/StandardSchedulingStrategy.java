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
package concoord.scheduling.strategy;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.concurrent.SchedulerFactory;
import concoord.concurrent.Trampoline;
import concoord.data.ConsumingFactory;
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.lang.Streamed;
import concoord.lang.Streamed.StreamedAwaitable;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class StandardSchedulingStrategy<T, M> implements SchedulingStrategy<T, M> {

  private final SchedulingStrategy<T, M> strategy;

  public StandardSchedulingStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingControl<? super M> schedulingControl,
      @NotNull Block<T, M> block) {
    new IfSomeOf(
        new IfNull("schedulerFactory", schedulerFactory),
        new IfNull("schedulingControl", schedulingControl),
        new IfNull("block", block)
    ).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.strategy = new Partial<T, M>(new TrampolineControl<M>(), block);
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.strategy = new Each<T, M>(new FactoryControl<M>(schedulerFactory), block);
    } else {
      this.strategy = new Partial<T, M>(schedulingControl, block);
    }
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    return strategy.schedule(message);
  }

  public void stopAll() throws Exception {
    strategy.stopAll();
  }

  public interface SchedulingControl<M> {

    @NotNull
    Scheduler schedulerFor(M message) throws Exception;
  }

  private static class TrampolineControl<M> implements SchedulingControl<M> {

    private final Trampoline trampoline = new Trampoline();

    @NotNull
    public Scheduler schedulerFor(M message) {
      return trampoline;
    }
  }

  private static class FactoryControl<M> implements SchedulingControl<M> {

    private final SchedulerFactory schedulerFactory;

    private FactoryControl(@NotNull SchedulerFactory schedulerFactory) {
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      return schedulerFactory.create();
    }
  }

  private static class Each<T, M> implements SchedulingStrategy<T, M> {

    private final SchedulingControl<? super M> schedulingControl;
    private final Block<T, M> block;

    public Each(@NotNull SchedulingControl<? super M> schedulingControl,
        @NotNull Block<T, M> block) {
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

  private static class Partial<T, M> implements SchedulingStrategy<T, M> {

    private final WeakHashMap<Scheduler, ScheduledTask<T, M>> tasks =
        new WeakHashMap<Scheduler, ScheduledTask<T, M>>();
    private final SchedulingControl<? super M> schedulingControl;
    private final Block<T, M> block;

    public Partial(@NotNull SchedulingControl<? super M> schedulingControl,
        @NotNull Block<T, M> block) {
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
}
