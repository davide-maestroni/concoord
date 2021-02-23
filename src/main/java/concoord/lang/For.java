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
package concoord.lang;

import concoord.concurrent.Awaitable;
import concoord.concurrent.Awaiter;
import concoord.concurrent.CombinedAwaiter;
import concoord.concurrent.NullaryAwaiter;
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.concurrent.UnaryAwaiter;
import concoord.flow.FlowControl;
import concoord.logging.DbgMessage;
import concoord.logging.ErrMessage;
import concoord.logging.InfMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.PrintIdentity;
import concoord.logging.WrnMessage;
import concoord.util.CircularQueue;
import concoord.util.assertion.IfNull;
import java.util.Arrays;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class For<T> implements Task<T> {

  private final Task<T> task;

  public For(@NotNull T... messages) {
    this(Arrays.asList(messages));
  }

  public For(@NotNull final Iterable<? extends T> messages) {
    new IfNull(messages, "messages").throwException();
    this.task = new Task<T>() {
      @NotNull
      public Awaitable<T> on(@NotNull Scheduler scheduler) {
        return new ForIterableAwaitable<T>(scheduler, messages.iterator());
      }
    };
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return task.on(scheduler);
  }

  private static class ForIterableAwaitable<T> implements Awaitable<T> {

    private final Logger forLogger = new Logger(Awaitable.class, this);
    private final CircularQueue<ForIterableFlowControl> flowControls = new CircularQueue<ForIterableFlowControl>();
    private final Scheduler scheduler;
    private final Iterator<? extends T> iterator;
    private ForIterableFlowControl currentFlowControl;
    private boolean stopped;

    private ForIterableAwaitable(@NotNull Scheduler scheduler, @NotNull Iterator<? extends T> iterator) {
      this.scheduler = scheduler;
      this.iterator = iterator;
      forLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
    }

    public void await(int maxEvents) {
      await(maxEvents, new DummyAwaiter<T>());
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      new IfNull(awaiter, "awaiter").throwException();
      scheduler.scheduleLow(new ForIterableFlowControl(maxEvents, awaiter));
    }

    public void await(int maxEvents, @NotNull UnaryAwaiter<? super T> messageAwaiter,
        @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter) {
      await(maxEvents, new CombinedAwaiter<T>(messageAwaiter, errorAwaiter, endAwaiter));
    }

    public void abort() {
      scheduler.scheduleHigh(new Runnable() {
        public void run() {
          stopped = true;
          forLogger.log(new InfMessage("[aborted]"));
        }
      });
    }

    private void nextFlowControl() {
      currentFlowControl = flowControls.poll();
      if (currentFlowControl != null) {
        scheduler.scheduleLow(currentFlowControl);
      } else {
        forLogger.log(new InfMessage("[settled]"));
      }
    }

    private class ForIterableFlowControl implements FlowControl<T>, Runnable {

      private final Logger flowLogger = new Logger(FlowControl.class, this);
      private final ReadState read = new ReadState();
      private final Awaiter<? super T> awaiter;
      private final int totEvents;
      private int events;
      private Runnable state;

      private ForIterableFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
        this.totEvents = maxEvents;
        this.events = maxEvents;
        this.awaiter = awaiter;
        this.state = new InitState();
        flowLogger.log(new DbgMessage("[initialized]"));
      }

      public void postOutput(T message) {
        flowLogger.log(new DbgMessage("[posting] new message: %d", totEvents - events));
        --events;
        try {
          awaiter.message(message);
        } catch (final Exception e) {
          sendError(e);
        }
      }

      public void postOutput(Awaitable<? extends T> awaitable) {
      }

      public void limitInputs(int maxEvents) {
      }

      public void stop() {
        stopped = true;
        flowLogger.log(new DbgMessage("[stopped]"));
        forLogger.log(new InfMessage("[complete]"));
      }

      public void run() {
        state.run();
      }

      private void sendError(@NotNull Throwable error) {
        stopped = true;
        try {
          awaiter.error(error);
        } catch (final Exception e) {
          forLogger.log(new ErrMessage(
              new LogMessage("failed to notify error to awaiter: %s", new PrintIdentity(awaiter)),
              e
          ));
        }
        forLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
      }

      private void sendEnd() {
        try {
          awaiter.end();
        } catch (final Exception e) {
          forLogger.log(new ErrMessage(
              new LogMessage("failed to notify end to awaiter: %s", new PrintIdentity(awaiter)),
              e
          ));
          sendError(e);
        }
        forLogger.log(new InfMessage("[ended]"));
      }

      private class InitState implements Runnable {

        public void run() {
          final ForIterableFlowControl thisFlowControl = ForIterableFlowControl.this;
          if (currentFlowControl == null) {
            currentFlowControl = thisFlowControl;
          }
          if (currentFlowControl != thisFlowControl) {
            flowControls.add(thisFlowControl);
            return;
          }
          if (stopped) {
            if (events >= 1) {
              sendEnd();
            }
            nextFlowControl();
          } else {
            state = read;
            flowLogger.log(new DbgMessage("[reading]"));
            state.run();
          }
        }
      }

      private class ReadState implements Runnable {

        public void run() {
          if (events < 1) {
            nextFlowControl();
          } else if (stopped) {
            sendEnd();
            nextFlowControl();
          } else {
            try {
              if (iterator.hasNext()) {
                postOutput(iterator.next());
              } else {
                stop();
              }
            } catch (final Exception e) {
              forLogger.log(new WrnMessage(new LogMessage("invocation failed with an exception"), e));
              sendError(e);
              events = 0;
            }
            scheduler.scheduleLow(currentFlowControl);
          }
        }
      }
    }
  }
}
