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
package concoord.scheduling.streaming;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Scheduler;
import concoord.lang.Parallel.Block;
import concoord.lang.Streamed;
import concoord.lang.Streamed.StreamedAwaitable;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class PartialStreaming<T, M> implements StreamingControl<T, M> {

  private final WeakHashMap<Scheduler, ScheduledTask<T, M>> tasks =
      new WeakHashMap<Scheduler, ScheduledTask<T, M>>();

  @NotNull
  public Awaitable<T> stream(@NotNull Scheduler scheduler, M message,
      @NotNull Block<T, ? super M> block) throws Exception {
    ScheduledTask<T, M> task = tasks.get(scheduler);
    if (task == null) {
      final StreamedAwaitable<M> input = new Streamed<M>().on(scheduler);
      final Awaitable<T> output = block.execute(input, scheduler);
      task = new ScheduledTask<T, M>(input, output);
      tasks.put(scheduler, task);
    }
    task.input().message(message);
    return task.output();
  }

  public void end() throws Exception {
    for (final ScheduledTask<T, M> task : tasks.values()) {
      task.input().end();
    }
  }

  public int inputEvents() {
    int events = 0;
    for (final ScheduledTask<T, M> task : tasks.values()) {
      final int requiredEvents = task.input().requiredEvents();
      if (requiredEvents < 0) {
        events = requiredEvents;
        break;
      }
      events += requiredEvents;
    }
    return events;
  }

  private static class ScheduledTask<T, M> {

    private final StreamedAwaitable<M> input;
    private final Awaitable<T> output;

    private ScheduledTask(@NotNull StreamedAwaitable<M> input, @NotNull Awaitable<T> output) {
      this.input = input;
      this.output = output;
    }

    @NotNull
    private StreamedAwaitable<M> input() {
      return input;
    }

    @NotNull
    private Awaitable<T> output() {
      return output;
    }
  }
}
