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
import java.util.ArrayList;
import org.jetbrains.annotations.NotNull;

public class LoadBalancing<M> implements SchedulingStrategy<M> {

  private final Trampoline trampoline = new Trampoline();
  private final ArrayList<Scheduler> schedulers = new ArrayList<Scheduler>();

  @NotNull
  public Scheduler schedulerFor(M message, int maxParallelism,
      @NotNull SchedulerFactory schedulerFactory) throws Exception {
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
