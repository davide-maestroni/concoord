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

import concoord.util.assertion.IfNull;
import java.util.Iterator;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class LastOf<M> implements Buffer<M> {

  private final WeakHashMap<LastIterator<M>, Void> iterators =
      new WeakHashMap<LastIterator<M>, Void>();
  private final Buffer<M> buffer;
  private final int maxMessages;

  public LastOf(int maxMessages, @NotNull Buffer<M> buffer) {
    new IfNull(buffer, "buffer").throwException();
    this.maxMessages = maxMessages;
    this.buffer = buffer;
  }

  public void add(M message) {
    final Buffer<M> buffer = this.buffer;
    buffer.add(message);
    if (buffer.size() > maxMessages) {
      buffer.remove(0);
      for (final LastIterator<M> iterator : iterators.keySet()) {
        iterator.advanceHead();
      }
    }
  }

  public void remove(int index) {
    buffer.remove(index);
  }

  public M get(int index) {
    return buffer.get(index);
  }

  public int size() {
    return buffer.size();
  }

  @NotNull
  public Iterator<M> iterator() {
    final LastIterator<M> iterator = new LastIterator<M>(buffer);
    iterators.put(iterator, null);
    return iterator;
  }

  private static class LastIterator<M> implements Iterator<M> {

    private final Buffer<M> buffer;
    private int index = 0;

    private LastIterator(@NotNull Buffer<M> buffer) {
      this.buffer = buffer;
    }

    public boolean hasNext() {
      return buffer.size() > index;
    }

    public M next() {
      return buffer.get(index++);
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    private void advanceHead() {
      index = Math.max(0, index - 1);
    }
  }
}
