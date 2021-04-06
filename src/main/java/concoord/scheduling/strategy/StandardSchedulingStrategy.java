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

import concoord.concurrent.Scheduler;
import concoord.concurrent.SchedulerFactory;
import concoord.concurrent.Trampoline;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import org.jetbrains.annotations.NotNull;

public class StandardSchedulingStrategy<M> implements SchedulingStrategy<M> {

  private final SchedulingStrategy<M> strategy;

  public StandardSchedulingStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory,
      @NotNull SchedulingStrategy<M> specializedStrategy) {
    new IfSomeOf(
        new IfNull("schedulerFactory", schedulerFactory),
        new IfNull("schedulerFactory", schedulerFactory)
    ).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.strategy = new TrampolineStrategy<M>();
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.strategy = new FactoryStrategy<M>(schedulerFactory);
    } else {
      this.strategy = specializedStrategy;
    }
  }

  @NotNull
  public Scheduler schedulerFor(M message) throws Exception {
    return strategy.schedulerFor(message);
  }

  private static class TrampolineStrategy<M> implements SchedulingStrategy<M> {

    private final Trampoline trampoline = new Trampoline();

    @NotNull
    public Scheduler schedulerFor(M message) {
      return trampoline;
    }
  }

  private static class FactoryStrategy<M> implements SchedulingStrategy<M> {

    private final SchedulerFactory schedulerFactory;

    private FactoryStrategy(@NotNull SchedulerFactory schedulerFactory) {
      this.schedulerFactory = schedulerFactory;
    }

    @NotNull
    public Scheduler schedulerFor(M message) throws Exception {
      return schedulerFactory.create();
    }
  }
}
