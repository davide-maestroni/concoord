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
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class RoundRobin<M> extends AbstractSchedulingStrategy<M> {

  public RoundRobin(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    super(maxParallelism, schedulerFactory);
  }

  @NotNull
  @Override
  SchedulerFactory create(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    return new RoundRobinFactory(maxParallelism, schedulerFactory);
  }

  private static class RoundRobinFactory implements SchedulerFactory {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;
    private int index;

    private RoundRobinFactory(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler create() throws Exception {
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
