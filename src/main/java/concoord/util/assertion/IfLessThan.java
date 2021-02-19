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

import org.jetbrains.annotations.Nullable;

public class IfLessThan extends IfAnyOf {

  public IfLessThan(Integer value, int limit) {
    this(value, "number", limit);
  }

  public IfLessThan(final Integer value, final String name, final int limit) {
    super(new IfNull(value, name), new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (value < limit) {
          return new IllegalArgumentException(name + " cannot be less than " + limit + ", but it was: " + value);
        }
        return null;
      }
    });
  }

  public IfLessThan(Long value, long limit) {
    this(value, "number", limit);
  }

  public IfLessThan(final Long value, final String name, final long limit) {
    super(new IfNull(value, name), new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (value < limit) {
          return new IllegalArgumentException(name + " cannot be less than " + limit + ", but it was: " + value);
        }
        return null;
      }
    });
  }

  public IfLessThan(Float value, int limit) {
    this(value, "number", limit);
  }

  public IfLessThan(final Float value, final String name, final int limit) {
    super(new IfNull(value, name), new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (value < limit) {
          return new IllegalArgumentException(name + " cannot be less than " + limit + ", but it was: " + value);
        }
        return null;
      }
    });
  }

  public IfLessThan(Double value, int limit) {
    this(value, "number", limit);
  }

  public IfLessThan(final Double value, final String name, final int limit) {
    super(new IfNull(value, name), new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (value < limit) {
          return new IllegalArgumentException(name + " cannot be less than " + limit + ", but it was: " + value);
        }
        return null;
      }
    });
  }
}
