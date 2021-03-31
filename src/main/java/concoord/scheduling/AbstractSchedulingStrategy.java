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
import org.jetbrains.annotations.NotNull;

abstract class AbstractSchedulingStrategy<M> implements SchedulingStrategy<M> {

  private final SchedulerFactory factory;

  AbstractSchedulingStrategy(int maxParallelism, @NotNull SchedulerFactory schedulerFactory) {
    new IfNull("schedulerFactory", schedulerFactory).throwException();
    if (maxParallelism == 0) {
      // no parallelism
      this.factory = new TrampolineFactory();
    } else if (maxParallelism < 0) {
      // infinite parallelism
      this.factory = schedulerFactory;
    } else {
      this.factory = create(maxParallelism, schedulerFactory);
    }
  }

  @NotNull
  public Scheduler nextScheduler(M message) throws Exception {
    return factory.create();
  }

  @NotNull
  abstract SchedulerFactory create(int maxParallelism, @NotNull SchedulerFactory schedulerFactory);
}
