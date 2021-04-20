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
import concoord.concurrent.Trampoline;
import concoord.lang.Parallel.SchedulingStrategy;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class LoadBalancing<M> implements SchedulingStrategy<M> {

  private final SchedulingStrategy<M> strategy;

  public LoadBalancing(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    if (maxParallelism < 0) {
      this.strategy = new FactoryStrategy<M>(schedulerFactory);
    } else if (maxParallelism == 0) {
      this.strategy = new TrampolineStrategy<M>();
    } else {
      this.strategy = new LoadBalancingStrategy<M>(maxParallelism, schedulerFactory);
    }
  }

  @NotNull
  public Scheduler schedulerFor(M message) throws Exception {
    return strategy.schedulerFor(message);
  }

  private static class LoadBalancingStrategy<M> implements SchedulingStrategy<M> {

    private final Trampoline trampoline = new Trampoline();
    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;

    public LoadBalancingStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      new IfNull("schedulerFactory", schedulerFactory).throwException();
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      Scheduler selected = trampoline;
      final ArrayList<Scheduler> schedulers = this.schedulers;
      if (schedulers.size() < maxParallelism) {
        selected = schedulerFactory.create();
        new IfNull("scheduler", selected).throwException();
        schedulers.add(selected);
      } else {
        int min = Integer.MAX_VALUE;
        for (final Scheduler scheduler : schedulers) {
          final int pendingCommands = scheduler.pendingCommands();
          if (pendingCommands < min) {
            min = pendingCommands;
            selected = scheduler;
          }
        }
      }
      return selected;
    }
  }
}
