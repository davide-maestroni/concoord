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
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class RoundRobin<M> implements SchedulingStrategy<M> {

  private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();
  private int index;

  @NotNull
  public Scheduler schedulerFor(M message, int maxParallelism,
      @NotNull SchedulerFactory schedulerFactory) throws Exception {
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
