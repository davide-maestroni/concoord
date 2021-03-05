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

public class Quinary<T1, T2, T3, T4, T5> {

  private final T1 first;
  private final T2 second;
  private final T3 third;
  private final T4 fourth;
  private final T5 fifth;

  public Quinary(T1 first, T2 second, T3 third, T4 fourth, T5 fifth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
    this.fifth = fifth;
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

  public T4 fourth() {
    return fourth;
  }

  public T5 fifth() {
    return fifth;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Quinary<?, ?, ?, ?, ?> quinary = (Quinary<?, ?, ?, ?, ?>) o;
    if (first != null ? !first.equals(quinary.first) : quinary.first != null) {
      return false;
    }
    if (second != null ? !second.equals(quinary.second) : quinary.second != null) {
      return false;
    }
    if (third != null ? !third.equals(quinary.third) : quinary.third != null) {
      return false;
    }
    if (fourth != null ? !fourth.equals(quinary.fourth) : quinary.fourth != null) {
      return false;
    }
    return fifth != null ? fifth.equals(quinary.fifth) : quinary.fifth == null;
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    result = 31 * result + (third != null ? third.hashCode() : 0);
    result = 31 * result + (fourth != null ? fourth.hashCode() : 0);
    result = 31 * result + (fifth != null ? fifth.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Quinary{first=" + first
        + ", second=" + second
        + ", third=" + third
        + ", fourth=" + fourth
        + ", fifth=" + fifth
        + '}';
  }
}
