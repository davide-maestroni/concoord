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
package concoord.concurrent;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import org.jetbrains.annotations.NotNull;

public class LazyExecutor implements Executor {

  private final ConcurrentLinkedQueue<Runnable> commandQueue =
      new ConcurrentLinkedQueue<Runnable>();

  public void execute(@NotNull Runnable command) {
    commandQueue.add(command);
  }

  public int advance(int maxCommands) {
    for (int i = 0; i < maxCommands; ++i) {
      Runnable task = commandQueue.poll();
      if (task != null) {
        task.run();
      } else {
        return i;
      }
    }
    return maxCommands;
  }
}
