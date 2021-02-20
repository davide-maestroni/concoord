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
package concoord.concurrent;

import concoord.logging.ErrMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.WrnMessage;
import concoord.util.assertion.IfNull;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public class ScheduledExecutor implements Scheduler {

  private static final int IDLE = 0;
  private static final int READING = 1;
  private static final int RUNNING = 2;

  private final ConcurrentLinkedQueue<Runnable> highQueue = new ConcurrentLinkedQueue<Runnable>();
  private final ConcurrentLinkedQueue<Runnable> lowQueue = new ConcurrentLinkedQueue<Runnable>();
  private final AtomicInteger status = new AtomicInteger(IDLE);
  private final Logger logger = new Logger(this);
  private final Executor executor;
  private final Runnable runner;

  public ScheduledExecutor(@NotNull Executor executor) {
    new IfNull(executor, "executor").throwException();
    this.executor = executor;
    this.runner = new Runnable() {
      public void run() {
        while (true) {
          status.set(READING);
          Runnable command = highQueue.poll();
          if (command == null) {
            command = lowQueue.poll();
            if (command == null) {
              // move to IDLE
              if (status.compareAndSet(READING, IDLE)) {
                return;
              } else {
                continue;
              }
            }
          }

          status.set(RUNNING);
          try {
            command.run();
          } catch (final Throwable t) {
            logger.log(new ErrMessage(new LogMessage("uncaught exception"), t));
            if (t instanceof RuntimeException) {
              throw (RuntimeException) t;
            } else {
              throw new RuntimeException(t);
            }
          }
        }
      }
    };
  }

  public ScheduledExecutor(@NotNull Executor executor, int throughput) {
    new IfNull(executor, "executor").throwException();
    this.executor = executor;
    if (throughput <= 1) {
      if (throughput <= 0) {
        logger.log(new WrnMessage("throughput changed to 1, it was: %d", throughput));
      }
      this.runner = new Runnable() {
        public void run() {
          while (true) {
            status.set(READING);
            Runnable command = highQueue.peek();
            if (command == null) {
              command = lowQueue.peek();
              if (command == null) {
                // move to IDLE
                if (!status.compareAndSet(READING, IDLE)) {
                  ScheduledExecutor.this.executor.execute(this);
                }
                return;
              } else {
                lowQueue.remove();
              }
            } else {
              highQueue.remove();
            }

            status.set(RUNNING);
            try {
              command.run();
            } catch (final Throwable t) {
              logger.log(new ErrMessage(new LogMessage("uncaught exception"), t));
              if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
              } else {
                throw new RuntimeException(t);
              }
            }
          }
        }
      };

    } else {
      final int max = throughput;
      this.runner = new Runnable() {
        public void run() {
          for (int i = 0; i < max; ++i) {
            status.set(READING);
            Runnable command = highQueue.peek();
            if (command == null) {
              command = lowQueue.peek();
              if (command == null) {
                // move to IDLE
                if (status.compareAndSet(READING, IDLE)) {
                  return;
                } else {
                  --i;
                  continue;
                }
              } else {
                lowQueue.remove();
              }
            } else {
              highQueue.remove();
            }

            status.set(RUNNING);
            try {
              command.run();
            } catch (final Throwable t) {
              logger.log(new ErrMessage(new LogMessage("uncaught exception"), t));
              if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
              } else {
                throw new RuntimeException(t);
              }
            }
          }
          Runnable command = highQueue.peek();
          if (command == null) {
            command = lowQueue.peek();
            if (command == null) {
              // move to IDLE
              if (!status.compareAndSet(READING, IDLE)) {
                ScheduledExecutor.this.executor.execute(this);
              }
            } else {
              ScheduledExecutor.this.executor.execute(this);
            }
          } else {
            ScheduledExecutor.this.executor.execute(this);
          }
        }
      };
    }
  }

  public void scheduleHigh(@NotNull Runnable command) {
    highQueue.offer(command);
    if (!status.compareAndSet(READING, RUNNING) && status.compareAndSet(IDLE, RUNNING)) {
      executor.execute(runner);
    }
  }

  public void scheduleLow(@NotNull Runnable command) {
    lowQueue.offer(command);
    if (!status.compareAndSet(READING, RUNNING) && status.compareAndSet(IDLE, RUNNING)) {
      executor.execute(runner);
    }
  }

  public int pendingCommands() {
    return highQueue.size() + lowQueue.size();
  }
}
