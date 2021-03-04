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

import java.util.ArrayList;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class First<M> implements Buffer<M> {

  private final ArrayList<M> data = new ArrayList<M>();
  private final int maxMessages;

  public First(int maxMessages) {
    this.maxMessages = maxMessages;
  }

  public void add(M message) {
    final ArrayList<M> data = this.data;
    if (data.size() < maxMessages) {
      data.add(message);
    }
  }

  @NotNull
  public Iterator<M> iterator() {
    return new LastIterator<M>(data);
  }

  private static class LastIterator<M> implements Iterator<M> {

    private final ArrayList<M> data;
    private int index = 0;

    private LastIterator(@NotNull ArrayList<M> data) {
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
  }
}
