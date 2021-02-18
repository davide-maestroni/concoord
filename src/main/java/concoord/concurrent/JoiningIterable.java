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

import concoord.util.assertion.IfNull;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class JoiningIterable<T> implements Iterable<T> {

  private final Awaitable<T> awaitable;
  private final int maxEvents;
  private final long nextTimeoutMs;
  private final long totalTimeoutMs;

  public JoiningIterable(@NotNull Awaitable<T> awaitable, int maxEvents, long nextTimeout,
      @NotNull TimeUnit timeUnit) {
    new IfNull(awaitable, "async").throwException();
    this.awaitable = awaitable;
    this.maxEvents = maxEvents;
    this.nextTimeoutMs = timeUnit.toMillis(nextTimeout);
    this.totalTimeoutMs = -1;
  }

  public JoiningIterable(@NotNull Awaitable<T> awaitable, int maxEvents, long nextTimeout,
      long totalTimeout,
      @NotNull TimeUnit timeUnit) {
    new IfNull(awaitable, "async").throwException();
    this.awaitable = awaitable;
    this.maxEvents = maxEvents;
    this.nextTimeoutMs = timeUnit.toMillis(nextTimeout);
    this.totalTimeoutMs = timeUnit.toMillis(totalTimeout);
  }

  @NotNull
  public Iterator<T> iterator() {
    return new JoiningIterator<T>(awaitable, maxEvents, nextTimeoutMs, totalTimeoutMs,
        TimeUnit.MILLISECONDS);
  }
}
