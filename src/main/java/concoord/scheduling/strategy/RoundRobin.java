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
import concoord.lang.Parallel.Block;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.scheduling.strategy.StandardSchedulingStrategy.SchedulingControl;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class RoundRobin<T, M> implements SchedulingStrategy<T, M> {

  private final StandardSchedulingStrategy<T, M> strategy;

  public RoundRobin(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull Block<T, M> block) {
    this.strategy = new StandardSchedulingStrategy<T, M>(
        maxParallelism,
        schedulerFactory,
        new RoundRobinControl<M>(maxParallelism, schedulerFactory),
        block
    );
  }

  @NotNull
  public Awaitable<T> schedule(M message) throws Exception {
    return strategy.schedule(message);
  }

  public void stopAll() throws Exception {
    strategy.stopAll();
  }

  private static class RoundRobinControl<M> implements SchedulingControl<M> {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;
    private int index;

    private RoundRobinControl(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      final int index = this.index;
      final ArrayList<Scheduler> schedulers = this.schedulers;
      final Scheduler scheduler;
      if (schedulers.size() == index) {
        scheduler = schedulerFactory.create();
        new IfNull("scheduler", scheduler).throwException();
        schedulers.add(scheduler);
      } else {
        scheduler = schedulers.get(index);
      }
      this.index = (index + 1) % maxParallelism;
      return scheduler;
    }
  }
}
