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

import java.util.Collection;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Buffered<M> implements Buffer<M> {

  private final Buffer<M> data;

  public Buffered(@NotNull final Collection<M> data) {
    this.data = new Buffer<M>() {
      public void add(M message) {
        data.add(message);
      }

      public void remove(int index) {
        final Iterator<M> iterator = data.iterator();
        for (int i = 0; i <= index; ++i) {
          iterator.next();
        }
        iterator.remove();
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

      @NotNull
      public Iterator<M> iterator() {
        return null;
      }
    };
  }

  public void add(M message) {

  }

  public void remove(int index) {

  }

  public M get(int index) {
    return null;
  }

  public int size() {
    return 0;
  }

  @NotNull
  public Iterator<M> iterator() {
    return null;
  }
}
