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
package concoord.util.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> {

  private final ArrayList<Object> values;

  public Tuple(T1 first) {
    this(Collections.singletonList(first));
  }

  public Tuple(T1 first, T2 second) {
    this(Arrays.asList(first, second));
  }

  public Tuple(T1 first, T2 second, T3 third) {
    this(Arrays.asList(first, second, third));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth) {
    this(Arrays.asList(first, second, third, fourth));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth) {
    this(Arrays.asList(first, second, third, fourth, fifth));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth) {
    this(Arrays.asList(first, second, third, fourth, fifth, sixth));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh) {
    this(Arrays.asList(first, second, third, fourth, fifth, sixth, seventh));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh, T8 eighth) {
    this(Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh, T8 eighth,
      T9 ninth) {
    this(Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth));
  }

  public Tuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh, T8 eighth,
      T9 ninth, T10 tenth) {
    this(Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth));
  }

  public Tuple(@NotNull Object... values) {
    this(Arrays.asList(values));
  }

  public Tuple(@NotNull List<?> values) {
    this.values = new ArrayList<Object>(values);
    for (int i = values.size(); i < 10; ++i) {
      this.values.add(null);
    }
  }

  @SuppressWarnings("unchecked")
  public T1 getFirst() {
    return (T1) values.get(0);
  }

  @SuppressWarnings("unchecked")
  public T2 getSecond() {
    return (T2) values.get(1);
  }

  @SuppressWarnings("unchecked")
  public T3 getThird() {
    return (T3) values.get(2);
  }

  @SuppressWarnings("unchecked")
  public T4 getFourth() {
    return (T4) values.get(3);
  }

  @SuppressWarnings("unchecked")
  public T5 getFifth() {
    return (T5) values.get(4);
  }

  @SuppressWarnings("unchecked")
  public T6 getSixth() {
    return (T6) values.get(5);
  }

  @SuppressWarnings("unchecked")
  public T7 getSeventh() {
    return (T7) values.get(6);
  }

  @SuppressWarnings("unchecked")
  public T8 getEighth() {
    return (T8) values.get(7);
  }

  @SuppressWarnings("unchecked")
  public T9 getNinth() {
    return (T9) values.get(8);
  }

  @SuppressWarnings("unchecked")
  public T10 getTenth() {
    return (T10) values.get(9);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(int index) {
    return (T) values.get(index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Tuple<?, ?, ?, ?, ?, ?, ?, ?, ?, ?> tuple = (Tuple<?, ?, ?, ?, ?, ?, ?, ?, ?, ?>) o;
    return values != null ? values.equals(tuple.values) : tuple.values == null;
  }

  @Override
  public int hashCode() {
    return values != null ? values.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "Tuple{values=" + values + '}';
  }
}
