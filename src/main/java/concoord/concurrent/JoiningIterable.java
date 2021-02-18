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

import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class JoiningIterable<T> implements Iterable<T> {

  private final Iterable<T> iterable;

  public JoiningIterable(@NotNull final Awaitable<T> awaitable, final int maxEvents, final long nextTimeout,
      @NotNull final TimeUnit timeUnit) {
    new IfAnyOf(
        new IfNull(awaitable, "awaitable"),
        new IfNull(timeUnit, "timeUnit")
    ).throwException();
    this.iterable = new Iterable<T>() {
      @NotNull
      public Iterator<T> iterator() {
        return new JoiningIterator<T>(awaitable, maxEvents, nextTimeout, timeUnit);
      }
    };
  }

  public JoiningIterable(@NotNull final Awaitable<T> awaitable, final int maxEvents, final long nextTimeout,
      final long totalTimeout, @NotNull final TimeUnit timeUnit) {
    new IfAnyOf(
        new IfNull(awaitable, "awaitable"),
        new IfNull(timeUnit, "timeUnit")
    ).throwException();
    this.iterable = new Iterable<T>() {
      @NotNull
      public Iterator<T> iterator() {
        return new JoiningIterator<T>(awaitable, maxEvents, nextTimeout, totalTimeout, timeUnit);
      }
    };
  }

  @NotNull
  public Iterator<T> iterator() {
    return iterable.iterator();
  }

  @NotNull
  public List<T> toList() {
    final ArrayList<T> messages = new ArrayList<T>();
    for (T message : this) {
      messages.add(message);
    }
    return messages;
  }
}
