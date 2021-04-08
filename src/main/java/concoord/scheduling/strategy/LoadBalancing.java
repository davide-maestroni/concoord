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
import concoord.util.assertion.IfContainsNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class LoadBalancing<T, M> implements SchedulingStrategy<T, M> {

  private final StandardSchedulingStrategy<T, M> strategy;

  public LoadBalancing(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull Block<T, M> block) {
    this.strategy = new StandardSchedulingStrategy<T, M>(
        maxParallelism,
        schedulerFactory,
        new LoadBalancingControl<M>(maxParallelism, schedulerFactory),
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

  private static class LoadBalancingControl<M> implements SchedulingControl<M> {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;
    private SchedulerFactory factoryState = new InitState();

    private LoadBalancingControl(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      return factoryState.create();
    }

    private class InitState implements SchedulerFactory {

      @NotNull
      public Scheduler create() throws Exception {
        final SchedulerFactory schedulerFactory = LoadBalancingControl.this.schedulerFactory;
        final ArrayList<Scheduler> schedulers = LoadBalancingControl.this.schedulers;
        for (int i = 0; i < maxParallelism; ++i) {
          final Scheduler scheduler = schedulerFactory.create();
          schedulers.add(scheduler);
        }
        new IfContainsNull("schedulers", schedulers).throwException();
        factoryState = new FactoryState();
        return factoryState.create();
      }
    }

    private class FactoryState implements SchedulerFactory {

      @NotNull
      @SuppressWarnings("ConstantConditions")
      public Scheduler create() {
        int min = Integer.MAX_VALUE;
        Scheduler selected = null;
        for (final Scheduler scheduler : schedulers) {
          final int pendingCommands = scheduler.pendingCommands();
          if (pendingCommands < min) {
            min = pendingCommands;
            selected = scheduler;
          }
        }
        return selected;
      }
    }
  }
}
