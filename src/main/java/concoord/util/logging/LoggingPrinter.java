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

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.jetbrains.annotations.NotNull;

public class LoggingPrinter implements LogPrinter {

  @NotNull
  public LoggingPrinter configure(@NotNull Level level) {
    final Logger logger = Logger.getLogger("");
    logger.setLevel(level);
    final Handler[] handlers = logger.getHandlers();
    if ((handlers != null) && (handlers.length > 0)) {
      final LoggingFormatter formatter = new LoggingFormatter();
      for (Handler handler : handlers) {
        handler.setFormatter(formatter);
      }
    } else {
      final ConsoleHandler handler = new ConsoleHandler();
      handler.setFormatter(new LoggingFormatter());
      logger.addHandler(handler);
    }
    return this;
  }

  public boolean canPrintDbg(@NotNull String name) {
    return Logger.getLogger(name).isLoggable(Level.FINE);
  }

  public boolean canPrintInf(@NotNull String name) {
    return Logger.getLogger(name).isLoggable(Level.INFO);
  }

  public boolean canPrintWrn(@NotNull String name) {
    return Logger.getLogger(name).isLoggable(Level.WARNING);
  }

  public boolean canPrintErr(@NotNull String name) {
    return Logger.getLogger(name).isLoggable(Level.SEVERE);
  }

  public void printDbg(@NotNull String name, String message) {
    Logger.getLogger(name).log(Level.FINE, message);
  }

  public void printInf(@NotNull String name, String message) {
    Logger.getLogger(name).log(Level.INFO, message);
  }

  public void printWrn(@NotNull String name, String message) {
    Logger.getLogger(name).log(Level.WARNING, message);
  }

  public void printErr(@NotNull String name, String message) {
    Logger.getLogger(name).log(Level.SEVERE, message);
  }

  private static class LoggingFormatter extends SimpleFormatter {

    @Override
    public String format(LogRecord record) {
      record.setSourceClassName(record.getLoggerName());
      record.setSourceMethodName(Integer.toString(record.getThreadID()));
      return super.format(record);
    }
  }
}
