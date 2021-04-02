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
package concoord.util.mutable;

public class Short extends Number {

  private short value;

  public Short() {
  }

  public Short(short value) {
    this.value = value;
  }

  @Override
  public short shortValue() {
    return value;
  }

  public int intValue() {
    return value;
  }

  public long longValue() {
    return value;
  }

  public float floatValue() {
    return value;
  }

  public double doubleValue() {
    return value;
  }

  public short getValue() {
    return value;
  }

  public void setValue(short value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Short)) {
      return false;
    }
    final Short other = (Short) o;
    return value == other.getValue();
  }

  @Override
  public int hashCode() {
    return value;
  }

  @Override
  public String toString() {
    return java.lang.Short.toString(value);
  }
}
