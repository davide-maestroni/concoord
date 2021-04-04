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

import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Consuming<M> implements Buffer<M> {

  private final Buffered<M> buffer;

  public Consuming() {
    buffer = new Buffered<M>();
  }

  public Consuming(int initialCapacity) {
    buffer = new Buffered<M>(initialCapacity);
  }

  public void add(M message) {
    buffer.add(message);
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
    return new ConsumingIterator<M>(buffer);
  }

  private static class ConsumingIterator<M> implements Iterator<M> {

    private final Buffered<M> buffer;
    private final Iterator<M> iterator;

    private ConsumingIterator(@NotNull Buffered<M> buffer) {
      this.buffer = buffer;
      this.iterator = buffer.iterator();
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public M next() {
      final M next = iterator.next();
      buffer.remove(0);
      return next;
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }
  }
}
