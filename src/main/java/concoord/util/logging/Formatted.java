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
package concoord.util.logging;

import java.util.Locale;
import org.jetbrains.annotations.NotNull;

public class Formatted {

  private static final Locale ROOT = new Locale("");

  private final Locale locale;
  private final String format;
  private final Object[] args;

  public Formatted(@NotNull String format, Object... args) {
    this(ROOT, format, args);
  }

  public Formatted(@NotNull Locale locale, @NotNull String format, Object... args) {
    this.locale = locale;
    this.format = format;
    this.args = args;
  }

  @Override
  public String toString() {
    return String.format(locale, format, args);
  }
}
