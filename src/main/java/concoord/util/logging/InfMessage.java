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
import org.jetbrains.annotations.Nullable;

public class InfMessage extends LogMessage {

  public InfMessage(@NotNull String message) {
    super(message);
  }

  public InfMessage(@NotNull Throwable error) {
    super(error);
  }

  public InfMessage(@NotNull String format, @NotNull Object... args) {
    super(format, args);
  }

  public InfMessage(@NotNull Locale locale, @NotNull String format, @NotNull Object... args) {
    super(locale, format, args);
  }

  public InfMessage(LogMessage wrapped) {
    super(wrapped);
  }

  public InfMessage(LogMessage wrapped, @NotNull Throwable error) {
    super(wrapped, error);
  }

  public InfMessage(LogMessage wrapped, @Nullable String message) {
    super(wrapped, message);
  }

  public InfMessage(LogMessage wrapped, @NotNull String format, @NotNull Object... args) {
    super(wrapped, format, args);
  }

  public InfMessage(LogMessage wrapped, @NotNull Locale locale, @NotNull String format,
      @NotNull Object... args) {
    super(wrapped, locale, format, args);
  }
}
