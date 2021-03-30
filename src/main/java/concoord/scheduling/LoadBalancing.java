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
import concoord.util.assertion.IfContainsNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class LoadBalancing<M> extends AbstractSchedulingStrategy<M> {

  public LoadBalancing(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    super(maxParallelism, schedulerFactory);
  }

  @NotNull
  @Override
  SchedulerFactory create(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    return new LoadBalancingFactory(maxParallelism, schedulerFactory);
  }

  private static class LoadBalancingFactory implements SchedulerFactory {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory schedulerFactory;
    private SchedulerFactory factoryState = new InitState();

    private LoadBalancingFactory(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
      this.maxParallelism = maxParallelism;
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler create() throws Exception {
      return factoryState.create();
    }

    private class InitState implements SchedulerFactory {

      @NotNull
      public Scheduler create() throws Exception {
        final SchedulerFactory schedulerFactory = LoadBalancingFactory.this.schedulerFactory;
        final ArrayList<Scheduler> schedulers = LoadBalancingFactory.this.schedulers;
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
