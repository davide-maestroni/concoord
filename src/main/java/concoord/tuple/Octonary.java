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

public class Octonary<T1, T2, T3, T4, T5, T6, T7, T8> {

  private final T1 first;
  private final T2 second;
  private final T3 third;
  private final T4 fourth;
  private final T5 fifth;
  private final T6 sixth;
  private final T7 seventh;
  private final T8 eighth;

  public Octonary(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh,
      T8 eighth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
    this.fifth = fifth;
    this.sixth = sixth;
    this.seventh = seventh;
    this.eighth = eighth;
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

  public T6 sixth() {
    return sixth;
  }

  public T7 seventh() {
    return seventh;
  }

  public T8 eighth() {
    return eighth;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Octonary<?, ?, ?, ?, ?, ?, ?, ?> octonary = (Octonary<?, ?, ?, ?, ?, ?, ?, ?>) o;
    if (first != null ? !first.equals(octonary.first) : octonary.first != null) {
      return false;
    }
    if (second != null ? !second.equals(octonary.second) : octonary.second != null) {
      return false;
    }
    if (third != null ? !third.equals(octonary.third) : octonary.third != null) {
      return false;
    }
    if (fourth != null ? !fourth.equals(octonary.fourth) : octonary.fourth != null) {
      return false;
    }
    if (fifth != null ? !fifth.equals(octonary.fifth) : octonary.fifth != null) {
      return false;
    }
    if (sixth != null ? !sixth.equals(octonary.sixth) : octonary.sixth != null) {
      return false;
    }
    if (seventh != null ? !seventh.equals(octonary.seventh) : octonary.seventh != null) {
      return false;
    }
    return eighth != null ? eighth.equals(octonary.eighth) : octonary.eighth == null;
  }

  @Override
  public int hashCode() {
    int result = first != null ? first.hashCode() : 0;
    result = 31 * result + (second != null ? second.hashCode() : 0);
    result = 31 * result + (third != null ? third.hashCode() : 0);
    result = 31 * result + (fourth != null ? fourth.hashCode() : 0);
    result = 31 * result + (fifth != null ? fifth.hashCode() : 0);
    result = 31 * result + (sixth != null ? sixth.hashCode() : 0);
    result = 31 * result + (seventh != null ? seventh.hashCode() : 0);
    result = 31 * result + (eighth != null ? eighth.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Octonary{first=" + first
        + ", second=" + second
        + ", third=" + third
        + ", fourth=" + fourth
        + ", fifth=" + fifth
        + ", sixth=" + sixth
        + ", seventh=" + seventh
        + ", eighth=" + eighth
        + '}';
  }
}
