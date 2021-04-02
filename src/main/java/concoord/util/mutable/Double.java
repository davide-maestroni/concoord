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

public class Double extends Number {

  private double value;

  public Double() {
  }

  public Double(double value) {
    this.value = value;
  }

  public int intValue() {
    return (int) value;
  }

  public long longValue() {
    return (long) value;
  }

  public float floatValue() {
    return (float) value;
  }

  public double doubleValue() {
    return value;
  }

  public double getValue() {
    return value;
  }

  public void setValue(double value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Double)) {
      return false;
    }
    final Double other = (Double) o;
    return value == other.getValue();
  }

  @Override
  public int hashCode() {
    final long bits = java.lang.Double.doubleToLongBits(value);
    return (int) (bits ^ (bits >>> 32));
  }

  @Override
  public String toString() {
    return java.lang.Double.toString(value);
  }
}
