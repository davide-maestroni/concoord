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

public class LoadBalancing<M> implements SchedulingStrategy<M> {

  private final SchedulerFactory factory;

  public LoadBalancing(int maxParallelism, @NotNull SchedulerFactory factory) {
    new IfNull("factory", factory).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.factory = new TrampolineFactory();
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.factory = factory;
    } else {
      this.factory = new LoadBalancingFactory(maxParallelism, factory);
    }
  }

  @NotNull
  public Scheduler nextScheduler(M message) throws Exception {
    return factory.create();
  }

  private static class LoadBalancingFactory implements SchedulerFactory {

    private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
    private final int maxParallelism;
    private final SchedulerFactory factory;
    private SchedulerFactory state = new InitState();

    private LoadBalancingFactory(int maxParallelism, @NotNull SchedulerFactory factory) {
      this.maxParallelism = maxParallelism;
      this.factory = factory;
    }

    @NotNull
    public Scheduler create() throws Exception {
      return state.create();
    }

    private class InitState implements SchedulerFactory {

      @NotNull
      @SuppressWarnings("ConstantConditions")
      public Scheduler create() throws Exception {
        for (int i = 0; i < maxParallelism; ++i) {
          final Scheduler scheduler = factory.create();
          if (scheduler == null) {
            throw new NullPointerException("scheduler cannot be null");
          }
          schedulers.add(scheduler);
        }
        state = new FactoryState();
        return state.create();
      }
    }

    private class FactoryState implements SchedulerFactory {

      @NotNull
      @SuppressWarnings("ConstantConditions")
      public Scheduler create() {
        int min = Integer.MAX_VALUE;
        Scheduler selected = null;
        for (Scheduler scheduler : schedulers) {
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
