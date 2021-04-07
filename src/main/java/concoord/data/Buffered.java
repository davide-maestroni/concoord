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
import concoord.util.collection.CircularQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.WeakHashMap;
import org.jetbrains.annotations.NotNull;

public class Buffered<M> implements Buffer<M> {

  private final Buffer<M> buffer;

  public Buffered() {
    this(new CircularQueue<M>());
  }

  public Buffered(int initialCapacity) {
    this(new CircularQueue<M>(initialCapacity));
  }

  public Buffered(@NotNull Collection<M> data) {
    new IfNull("data", data).throwException();
    this.buffer = new BufferedCollection<M>(data);
  }

  public Buffered(@NotNull List<M> data) {
    new IfNull("data", data).throwException();
    this.buffer = new BufferedList<M>(data);
  }

  public Buffered(@NotNull Queue<M> data) {
    new IfNull("data", data).throwException();
    this.buffer = new BufferedQueue<M>(data);
  }

  public Buffered(@NotNull CircularQueue<M> data) {
    new IfNull("data", data).throwException();
    this.buffer = new BufferedCircularQueue<M>(data);
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
    return buffer.iterator();
  }

  private static abstract class AbstractBuffer<M> implements Buffer<M> {

    private final WeakHashMap<BufferedIterator<M>, Void> iterators =
        new WeakHashMap<BufferedIterator<M>, Void>();

    public void remove(int index) {
      for (final BufferedIterator<M> iterator : iterators.keySet()) {
        iterator.advance(index);
      }
    }

    @NotNull
    public final Iterator<M> iterator() {
      final BufferedIterator<M> iterator = new BufferedIterator<M>(this);
      iterators.put(iterator, null);
      return iterator;
    }
  }

  private static class BufferedCollection<M> extends AbstractBuffer<M> {

    private final Collection<M> data;

    private BufferedCollection(@NotNull Collection<M> data) {
      this.data = data;
    }

    public void add(M message) {
      data.add(message);
    }

    @Override
    public void remove(int index) {
      final Iterator<M> iterator = data.iterator();
      for (int i = 0; i <= index; ++i) {
        iterator.next();
      }
      iterator.remove();
      super.remove(index);
    }

    public M get(int index) {
      final Iterator<M> iterator = data.iterator();
      for (int i = 0; i < index; ++i) {
        iterator.next();
      }
      return iterator.next();
    }

    public int size() {
      return data.size();
    }
  }

  private static class BufferedList<M> extends AbstractBuffer<M> {

    private final List<M> data;

    private BufferedList(@NotNull List<M> data) {
      this.data = data;
    }

    public void add(M message) {
      data.add(message);
    }

    @Override
    public void remove(int index) {
      data.remove(index);
      super.remove(index);
    }

    public M get(int index) {
      return data.get(index);
    }

    public int size() {
      return data.size();
    }
  }

  private static class BufferedQueue<M> extends AbstractBuffer<M> {

    private final Queue<M> data;

    private BufferedQueue(@NotNull Queue<M> data) {
      this.data = data;
    }

    public void add(M message) {
      data.add(message);
    }

    @Override
    public void remove(int index) {
      if (index == 0) {
        data.remove();
      } else {
        final Iterator<M> iterator = data.iterator();
        for (int i = 0; i <= index; ++i) {
          iterator.next();
        }
        iterator.remove();
      }
      super.remove(index);
    }

    public M get(int index) {
      if (index == 0) {
        return data.peek();
      }
      final Iterator<M> iterator = data.iterator();
      for (int i = 0; i < index; ++i) {
        iterator.next();
      }
      return iterator.next();
    }

    public int size() {
      return data.size();
    }
  }

  private static class BufferedCircularQueue<M> extends AbstractBuffer<M> {

    private final CircularQueue<M> data;

    private BufferedCircularQueue(@NotNull CircularQueue<M> data) {
      this.data = data;
    }

    public void add(M message) {
      data.add(message);
    }

    @Override
    public void remove(int index) {
      if (index == 0) {
        data.remove();
      } else {
        data.remove(index);
      }
      super.remove(index);
    }

    public M get(int index) {
      return data.get(index);
    }

    public int size() {
      return data.size();
    }
  }

  private static class BufferedIterator<M> implements Iterator<M> {

    private final Buffer<M> data;
    private int index = 0;

    private BufferedIterator(@NotNull Buffer<M> buffer) {
      this.data = buffer;
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

    private void advance(int offset) {
      if (offset <= index) {
        index = Math.max(0, index - 1);
      }
    }
  }
}
