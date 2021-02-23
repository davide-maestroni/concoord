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
package concoord.logging;

public class SafeString {

  // TODO: 23/02/21 remove?

  private final Object toString;

  public SafeString(Object toString) {
    this.toString = toString;
  }

  @Override
  public String toString() {
    try {
      return String.valueOf(toString);
    } catch (final Exception ignored) {
      return null;
    }
  }
}
