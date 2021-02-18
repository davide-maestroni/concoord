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

import java.util.Locale;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class LogMessage {

  private final Object toString;
  private final Throwable error;

  public LogMessage(@NotNull String message) {
    this((Object) message);
  }

  public LogMessage(@NotNull Throwable error) {
    this(new PrintStackTrace(error));
  }

  public LogMessage(@NotNull String format, @NotNull Object... args) {
    this(new Formatted(format, args));
  }

  public LogMessage(@NotNull Locale locale, @NotNull String format, @NotNull Object... args) {
    this(new Formatted(locale, format, args));
  }

  public LogMessage(LogMessage wrapped, @NotNull Throwable error) {
    this(new Formatted("%s\n%s", wrapped, new PrintStackTrace(error)), error);
  }

  public LogMessage(LogMessage wrapped, @NotNull String message) {
    this("%s - %s", wrapped, message);
  }

  public LogMessage(LogMessage wrapped, @NotNull String format, @NotNull Object... args) {
    this("%s - %s", wrapped, new LogMessage(format, args));
  }

  public LogMessage(LogMessage wrapped, @NotNull Locale locale, @NotNull String format,
      @NotNull Object... args) {
    this("%s - %s", wrapped, new LogMessage(locale, format, args));
  }

  private LogMessage(@NotNull Object toString) {
    this.toString = toString;
    this.error = null;
  }

  private LogMessage(@NotNull Object toString, @NotNull Throwable error) {
    this.toString = toString;
    this.error = error;
  }

  public boolean hasError() {
    return getError() != null;
  }

  @Nullable
  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return toString.toString();
  }
}
