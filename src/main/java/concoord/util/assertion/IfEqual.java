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
package concoord.util.assertion;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IfEqual<T> extends AbstractPrecondition {

  private final T object;
  private final String name;
  private final T value;

  public IfEqual(T object, T value) {
    this(object, "object", value);
  }

  public IfEqual(T object, String name, T value) {
    this.object = object;
    this.name = name;
    this.value = value;
  }

  @Nullable
  public RuntimeException getException() {
    if ((object == value) || ((object != null) && object.equals(value))) {
      return buildException();
    }
    return null;
  }

  @NotNull
  private IllegalArgumentException buildException() {
    return new IllegalArgumentException(name + " cannot be equal to: " + value);
  }
}
