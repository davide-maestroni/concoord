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

import concoord.util.collection.CircularQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class Last<M> implements Buffer<M> {

  private final WeakHashMap<LastIterator<M>, Void> iterators =
      new WeakHashMap<LastIterator<M>, Void>();
  private final CircularQueue<M> data = new CircularQueue<M>();
  private final int maxMessages;

  public Last(int maxMessages) {
    this.maxMessages = maxMessages;
  }

  public void add(M message) {
    final CircularQueue<M> data = this.data;
    data.offer(message);
    if (data.size() > maxMessages) {
      data.remove();
      for (final LastIterator<M> iterator : iterators.keySet()) {
        iterator.advanceHead();
      }
    }
  }

  @NotNull
  public Iterator<M> iterator() {
    final LastIterator<M> iterator = new LastIterator<M>(data);
    iterators.put(iterator, null);
    return iterator;
  }

  private static class LastIterator<M> implements Iterator<M> {

    private final CircularQueue<M> data;
    private int index = 0;

    private LastIterator(@NotNull CircularQueue<M> data) {
      this.data = data;
    }

    public boolean hasNext() {
      return data.size() > index;
    }

    public M next() {
      return data.get(index++);
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    private void advanceHead() {
      index = Math.max(0, index - 1);
    }
  }
}
