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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import org.jetbrains.annotations.NotNull;

public class Logger {

  private static final Object printersMutex = new Object();
  private static volatile HashMap<String, HashSet<LogPrinter>> printers =
      new HashMap<String, HashSet<LogPrinter>>();

  private final String name;
  private volatile ArrayList<String> names;

  public Logger() {
    this("");
  }

  public Logger(@NotNull Class<?> context) {
    this(context.getName());
  }

  public Logger(@NotNull Object context) {
    this(context.getClass(), context);
  }

  public Logger(@NotNull Class<?> parent, @NotNull Object context) {
    this(parent.getName() + "." + System.identityHashCode(context));
  }

  private Logger(@NotNull String name) {
    this.name = name;
  }

  @NotNull
  public String getName() {
    return name;
  }

  public void addPrinter(@NotNull LogPrinter printer) {
    // copy on write
    final String name = this.name;
    synchronized (printersMutex) {
      final HashMap<String, HashSet<LogPrinter>> newPrinters =
          new HashMap<String, HashSet<LogPrinter>>(printers);
      HashSet<LogPrinter> logPrinters = newPrinters.get(name);
      if (logPrinters == null) {
        logPrinters = new HashSet<LogPrinter>();
        newPrinters.put(name, logPrinters);
      }
      logPrinters.add(printer);
      printers = newPrinters;
    }
  }

  public void removePrinter(@NotNull LogPrinter printer) {
    // copy on write
    final String name = this.name;
    synchronized (printersMutex) {
      final HashMap<String, HashSet<LogPrinter>> newPrinters =
          new HashMap<String, HashSet<LogPrinter>>(printers);
      HashSet<LogPrinter> logPrinters = newPrinters.get(name);
      if (logPrinters != null) {
        logPrinters.remove(printer);
        if (logPrinters.isEmpty()) {
          newPrinters.remove(name);
        }
        printers = newPrinters;
      }
    }
  }

  public void clearPrinters() {
    // copy on write
    synchronized (printersMutex) {
      final HashMap<String, HashSet<LogPrinter>> newPrinters =
          new HashMap<String, HashSet<LogPrinter>>(printers);
      if (newPrinters.remove(name) != null) {
        printers = newPrinters;
      }
    }
  }

  public void log(@NotNull DbgMessage message) {
    String messageText = null;
    final String name = this.name;
    final HashMap<String, HashSet<LogPrinter>> printers = Logger.printers;
    for (String path : getNames()) {
      final HashSet<LogPrinter> logPrinters = printers.get(path);
      if (logPrinters != null) {
        for (LogPrinter logPrinter : logPrinters) {
          if (logPrinter.canPrintDbg(name)) {
            if (messageText == null) {
              messageText = message.toString();
            }
            logPrinter.printDbg(name, messageText);
          }
        }
      }
    }
  }

  public void log(@NotNull InfMessage message) {
    String messageText = null;
    final String name = this.name;
    final HashMap<String, HashSet<LogPrinter>> printers = Logger.printers;
    for (String path : getNames()) {
      final HashSet<LogPrinter> logPrinters = printers.get(path);
      if (logPrinters != null) {
        for (LogPrinter logPrinter : logPrinters) {
          if (logPrinter.canPrintInf(name)) {
            if (messageText == null) {
              messageText = message.toString();
            }
            logPrinter.printInf(name, messageText);
          }
        }
      }
    }
  }

  public void log(@NotNull WrnMessage message) {
    String messageText = null;
    final String name = this.name;
    final HashMap<String, HashSet<LogPrinter>> printers = Logger.printers;
    for (String path : getNames()) {
      final HashSet<LogPrinter> logPrinters = printers.get(path);
      if (logPrinters != null) {
        for (LogPrinter logPrinter : logPrinters) {
          if (logPrinter.canPrintWrn(name)) {
            if (messageText == null) {
              messageText = message.toString();
            }
            logPrinter.printWrn(name, messageText);
          }
        }
      }
    }
  }

  public void log(@NotNull ErrMessage message) {
    String messageText = null;
    final String name = this.name;
    final HashMap<String, HashSet<LogPrinter>> printers = Logger.printers;
    for (String path : getNames()) {
      final HashSet<LogPrinter> logPrinters = printers.get(path);
      if (logPrinters != null) {
        for (LogPrinter logPrinter : logPrinters) {
          if (logPrinter.canPrintErr(name)) {
            if (messageText == null) {
              messageText = message.toString();
            }
            logPrinter.printErr(name, messageText);
          }
        }
      }
    }
  }

  @NotNull
  private ArrayList<String> getNames() {
    if (names == null) {
      final ArrayList<String> paths = new ArrayList<String>();
      paths.add(""); // add root
      final StringBuilder builder = new StringBuilder();
      for (String part : name.split("\\.")) {
        if (builder.length() > 0) {
          builder.append('.');
        }
        builder.append(part);
        paths.add(0, builder.toString());
      }
      names = paths;
    }
    return names;
  }
}
