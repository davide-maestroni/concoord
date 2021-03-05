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

public class Ternary<T1, T2, T3> {

  private final T1 first;
  private final T2 second;
  private final T3 third;

  public Ternary(T1 first, T2 second, T3 third) {
    this.first = first;
    this.second = second;
    this.third = third;
  }

  public T1 first() {
    return first;
  }

  public T2 second() {
    return second;
  }

  public T3 third() {
    return third;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Ternary<?, ?, ?> ternary = (Ternary<?, ?, ?>) o;
    if (first != null ? !first.equals(ternary.first) : ternary.first != null) {
      return false;
    }
    if (second != null ? !second.equals(ternary.second) : ternary.second != null) {
      return false;
    }
    return third != null ? third.equals(ternary.third) : ternary.third == null;
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    result = 31 * result + (third != null ? third.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Ternary{first=" + first
        + ", second=" + second
        + ", third=" + third
        + '}';
  }
}
