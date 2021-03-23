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
import org.jetbrains.annotations.NotNull;

public class Drained<M> implements Buffer<M> {

  private final Buffer<M> buffer;

  public Drained() {
    this(new Buffered<M>());
  }

  public Drained(int initialCapacity) {
    this(new Buffered<M>(initialCapacity));
  }

  public Drained(@NotNull Buffer<M> buffer) {
    new IfNull("buffer", buffer).throwException();
    this.buffer = buffer;
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
    final Iterator<M> iterator = buffer.iterator();
    while (iterator.hasNext()) {
      iterator.next();
    }
    return iterator;
  }
}
