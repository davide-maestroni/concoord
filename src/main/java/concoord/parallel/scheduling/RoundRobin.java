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
package concoord.parallel.scheduling;

import concoord.concurrent.Scheduler;
import concoord.concurrent.SchedulerFactory;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class RoundRobin<M> implements SchedulingStrategy<M> {

  private final SchedulingStrategy<M> strategy;

  public RoundRobin(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    if (maxParallelism < 0) {
      this.strategy = new FactoryStrategy<M>(schedulerFactory);
    } else if (maxParallelism == 0) {
      this.strategy = new TrampolineStrategy<M>();
    } else {
      this.strategy = new RoundRobinStrategy<M>(maxParallelism, schedulerFactory);
    }
  }

  @NotNull
  public Scheduler schedulerFor(M message) throws Exception {
    return strategy.schedulerFor(message);
  }

  private static class RoundRobinStrategy<M> implements SchedulingStrategy<M> {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;
    private int index;

    public RoundRobinStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      new IfNull("schedulerFactory", schedulerFactory).throwException();
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      final int maxParallelism = this.maxParallelism;
      final ArrayList<Scheduler> schedulers = this.schedulers;
      if (schedulers.size() < maxParallelism) {
        index = schedulers.size();
        final Scheduler scheduler = schedulerFactory.create();
        new IfNull("scheduler", scheduler).throwException();
        schedulers.add(scheduler);
      } else {
        index = (index + 1) % maxParallelism;
      }
      return schedulers.get(index);
    }
  }
}
