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

public class Novenary<T1, T2, T3, T4, T5, T6, T7, T8, T9> {

  private final T1 first;
  private final T2 second;
  private final T3 third;
  private final T4 fourth;
  private final T5 fifth;
  private final T6 sixth;
  private final T7 seventh;
  private final T8 eighth;
  private final T9 ninth;

  public Novenary(T1 first, T2 second, T3 third, T4 fourth, T5 fifth, T6 sixth, T7 seventh, T8 eighth, T9 ninth) {
    this.first = first;
    this.second = second;
    this.third = third;
    this.fourth = fourth;
    this.fifth = fifth;
    this.sixth = sixth;
    this.seventh = seventh;
    this.eighth = eighth;
    this.ninth = ninth;
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

  public T9 ninth() {
    return ninth;
  }
}
