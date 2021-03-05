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
package concoord.tuple;

import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class Nary<T> {

  private final List<T> elements;

  public Nary(@NotNull List<T> elements) {
    new IfNull(elements, "elements").throwException();
    this.elements = elements;
  }

  public Nary(@NotNull T... elements) {
    this.elements = Arrays.asList(elements);
  }

  public T get(int index) {
    return elements.get(index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Nary<?> nary = (Nary<?>) o;
    return elements != null ? elements.equals(nary.elements) : nary.elements == null;
  }

  @Override
  public int hashCode() {
    return elements != null ? elements.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Nary{elements=" + elements + '}';
  }
}
