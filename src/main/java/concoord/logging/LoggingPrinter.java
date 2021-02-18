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

import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import org.jetbrains.annotations.NotNull;

public class LoggingPrinter implements LogPrinter {

  public void applyDefaultConfiguration(@NotNull concoord.logging.Logger logger, @NotNull Level level) {
    final Logger rootLogger = Logger.getLogger(logger.getName());
    rootLogger.setLevel(level);
    final Handler[] handlers = rootLogger.getHandlers();
    if (handlers != null) {
      final LoggingFormatter formatter = new LoggingFormatter();
      for (Handler handler : handlers) {
        handler.setFormatter(formatter);
      }
    } else {
      final ConsoleHandler handler = new ConsoleHandler();
      handler.setFormatter(new LoggingFormatter());
      rootLogger.addHandler(handler);
    }
    logger.addPrinter(this);
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

  public void printDbg(@NotNull String name, String message, Throwable error) {
    Logger.getLogger(name).log(Level.FINE, message, error);
  }

  public void printInf(@NotNull String name, String message, Throwable error) {
    Logger.getLogger(name).log(Level.INFO, message, error);
  }

  public void printWrn(@NotNull String name, String message, Throwable error) {
    Logger.getLogger(name).log(Level.WARNING, message, error);
  }

  public void printErr(@NotNull String name, String message, Throwable error) {
    Logger.getLogger(name).log(Level.SEVERE, message, error);
  }

  private static class LoggingFormatter extends SimpleFormatter {

    @Override
    public String format(LogRecord record) {
      record.setSourceClassName(record.getLoggerName());
      record.setSourceMethodName(Integer.toString(record.getThreadID()));
      record.setThrown(null);
      return super.format(record);
    }
  }
}
