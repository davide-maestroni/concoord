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
import org.jetbrains.annotations.NotNull;

public class SingleStreaming<T, M> implements StreamingControl<T, M> {

  @NotNull
  public Awaitable<T> stream(@NotNull Scheduler scheduler, M message,
      @NotNull Block<T, ? super M> block) throws Exception {
    final StreamedAwaitable<M> input = new Streamed<M>().on(scheduler);
    final Awaitable<T> output = block.execute(input, scheduler);
    input.message(message);
    input.end();
    return output;
  }

  public void end() {
  }

  public int inputEvents() {
    return 0;
  }
}
