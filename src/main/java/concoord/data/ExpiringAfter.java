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
package concoord.data;

import concoord.tuple.Binary;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class ExpiringAfter<M> implements Buffer<M> {

  private final WeakHashMap<ExpiringIterator<M>, Void> iterators =
      new WeakHashMap<ExpiringIterator<M>, Void>();
  private final Buffer<Binary<Long, M>> buffer;
  private final TimeUnit timeUnit;
  private final long timeout;

  public ExpiringAfter(long timeout, @NotNull TimeUnit timeUnit,
      @NotNull Buffer<Binary<Long, M>> buffer) {
    new IfSomeOf(
        new IfNull(buffer, "buffer"),
        new IfNull(timeUnit, "timeUnit")
    ).throwException();
    this.timeout = timeout;
    this.timeUnit = timeUnit;
    this.buffer = buffer;
  }

  public void add(M message) {
    buffer.add(
        new Binary<Long, M>(System.currentTimeMillis() + timeUnit.toMillis(timeout), message)
    );
    pruneExpired();
  }

  public void remove(int index) {
    buffer.remove(index);
    pruneExpired();
  }

  public M get(int index) {
    return buffer.get(index).second();
  }

  public int size() {
    return buffer.size();
  }

  @NotNull
  public Iterator<M> iterator() {
    final ExpiringIterator<M> iterator = new ExpiringIterator<M>(buffer);
    iterators.put(iterator, null);
    return iterator;
  }

  private void pruneExpired() {
    final long now = System.currentTimeMillis();
    for (int i = 0; i < buffer.size(); ++i) {
      final Binary<Long, M> binary = buffer.get(i);
      if (binary.first() <= now) {
        // expired
        buffer.remove(i);
        for (final ExpiringIterator<M> iterator : iterators.keySet()) {
          iterator.advanceHead(i);
        }
        --i;
      }
    }
  }

  private static class ExpiringIterator<M> implements Iterator<M> {

    private final Buffer<Binary<Long, M>> buffer;
    private int index = 0;

    private ExpiringIterator(@NotNull Buffer<Binary<Long, M>> buffer) {
      this.buffer = buffer;
    }

    public boolean hasNext() {
      return buffer.size() > index;
    }

    public M next() {
      return buffer.get(index++).second();
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    private void advanceHead(int offset) {
      if (offset <= index) {
        index = Math.max(0, index - 1);
      }
    }
  }
}
