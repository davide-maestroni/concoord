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

import concoord.concurrent.Scheduler;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class RoundRobin<M> implements SchedulingStrategy<M> {

  private final SchedulerFactory factory;

  public RoundRobin(int maxParallelism, @NotNull SchedulerFactory factory) {
    new IfNull("factory", factory).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.factory = new TrampolineFactory();
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.factory = factory;
    } else {
      this.factory = new RoundRobinFactory(maxParallelism, factory);
    }
  }

  @NotNull
  public Scheduler nextScheduler(M message) throws Exception {
    return factory.create();
  }

  private static class RoundRobinFactory implements SchedulerFactory {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory factory;
    private int index;

    private RoundRobinFactory(int maxParallelism, @NotNull SchedulerFactory factory) {
      this.maxParallelism = maxParallelism;
      this.factory = factory;
    }

    @NotNull
    public Scheduler create() throws Exception {
      final int index = this.index;
      final ArrayList<Scheduler> schedulers = this.schedulers;
      final Scheduler scheduler;
      if (schedulers.size() == index) {
        scheduler = factory.create();
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
