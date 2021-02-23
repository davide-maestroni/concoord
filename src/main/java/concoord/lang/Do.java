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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.flow.FlowControl;
import concoord.flow.NullaryInvocation;
import concoord.flow.Result;
import concoord.logging.ErrMessage;
import concoord.logging.InfMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.PrintIdentity;
import concoord.logging.WrnMessage;
import concoord.util.CircularQueue;
import concoord.util.assertion.IfNull;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Do<T> implements Task<T> {

  private static final Object NULL = new Object();

  private final NullaryInvocation<T> invocation;

  public Do(@NotNull NullaryInvocation<T> invocation) {
    new IfNull(invocation, "invocation").throwException();
    this.invocation = invocation;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new DoAwaitable<T>(scheduler, invocation);
  }

  private static class DoAwaitable<T> implements Awaitable<T> {

    private final Logger logger = new Logger(Awaitable.class, this);
    private final CircularQueue<DoFlowControl> flowControls = new CircularQueue<DoFlowControl>();
    private final CircularQueue<Awaitable<? extends T>> outputs = new CircularQueue<Awaitable<? extends T>>();
    private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final NullaryInvocation<T> invocation;
    private DoFlowControl currentFlowControl;
    private boolean stopped;

    private DoAwaitable(@NotNull Scheduler scheduler, @NotNull NullaryInvocation<T> invocation) {
      this.scheduler = scheduler;
      this.invocation = invocation;
      logger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
    }

    public void await(int maxEvents) {
      await(maxEvents, new DummyAwaiter<T>());
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      new IfNull(awaiter, "awaiter").throwException();
      scheduler.scheduleLow(new DoFlowControl(maxEvents, awaiter));
    }

    public void abort() {
      scheduler.scheduleHigh(new Runnable() {
        public void run() {
          stopped = true;
          outputs.clear();
          logger.log(new InfMessage("[aborted]"));
        }
      });
    }

    private void nextFlowControl() {
      currentFlowControl = flowControls.poll();
      if (currentFlowControl != null) {
        scheduler.scheduleLow(currentFlowControl);
      } else {
        logger.log(new InfMessage("[settled]"));
      }
    }

    private class DoFlowControl implements FlowControl<T>, Awaiter<T>, Runnable {

      private final ReadState read = new ReadState();
      private final WriteState write = new WriteState();
      private final EndState end = new EndState();
      private final Awaiter<? super T> awaiter;
      private int maxEvents = 1;
      private int events;
      private Runnable state;
      private int posts;

      private DoFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
        this.events = maxEvents;
        this.awaiter = awaiter;
        this.state = new InitState();
      }

      public void postOutput(T message) {
        if (++posts > 1) {
          throw new IllegalStateException("multiple outputs posted by the result");
        }
        --events;
        try {
          awaiter.message(message);
        } catch (final Exception e) {
          sendError(e);
        }
      }

      public void postOutput(Awaitable<? extends T> awaitable) {
        if (++posts > 1) {
          throw new IllegalStateException("multiple outputs posted by the result");
        }
        if (awaitable != null) {
          outputs.add(awaitable);
        }
      }

      public void limitInputs(int maxEvents) {
        this.maxEvents = maxEvents;
      }

      public void stop() {
        stopped = true;
        logger.log(new InfMessage("[complete]"));
      }

      public void message(T message) {
        messages.add(message != null ? message : NULL);
        scheduler.scheduleLow(this);
      }

      public void error(final Throwable error) {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            sendError(error);
            nextFlowControl();
          }
        });
      }

      public void end() {
        scheduler.scheduleLow(end);
      }

      public void run() {
        state.run();
      }

      private void sendError(@NotNull Throwable throwable) {
        stopped = true;
        outputs.clear();
        try {
          awaiter.error(throwable);
        } catch (final Exception e) {
          logger.log(new ErrMessage(
              new LogMessage("failed to notify error to awaiter: %s", new PrintIdentity(awaiter)),
              e
          ));
        }
        logger.log(new InfMessage("[failed]"));
      }

      private void sendEnd() {
        try {
          awaiter.end();
        } catch (final Exception e) {
          logger.log(new ErrMessage(
              new LogMessage("failed to notify end to awaiter: %s", new PrintIdentity(awaiter)),
              e
          ));
          sendError(e);
        }
        logger.log(new InfMessage("[ended]"));
      }

      private class InitState implements Runnable {

        public void run() {
          final DoFlowControl thisFlowControl = DoFlowControl.this;
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
            state.run();
          }
        }
      }

      private class ReadState implements Runnable {

        public void run() {
          if (events < 1) {
            nextFlowControl();
          } else {
            final DoFlowControl currentFlowControl = DoAwaitable.this.currentFlowControl;
            final CircularQueue<Awaitable<? extends T>> outputs = DoAwaitable.this.outputs;
            Awaitable<? extends T> awaitable = outputs.poll();
            if (awaitable != null) {
              state = write;
              awaitable.await(Math.min(events, maxEvents), currentFlowControl);
            } else if (stopped) {
              sendEnd();
              nextFlowControl();
            } else {
              try {
                final Result<T> result = invocation.call();
                posts = 0;
                result.apply(currentFlowControl);
                awaitable = outputs.poll();
                if (awaitable != null) {
                  state = write;
                  awaitable.await(Math.min(events, maxEvents), currentFlowControl);
                  return;
                }
              } catch (final Exception e) {
                logger.log(new WrnMessage(new LogMessage("invocation failed with an exception"), e));
                sendError(e);
                events = 0;
              }
              scheduler.scheduleLow(currentFlowControl);
            }
          }
        }
      }

      private class WriteState implements Runnable {

        @SuppressWarnings("unchecked")
        public void run() {
          final Object message = messages.poll();
          if (message != null) {
            postOutput(message != NULL ? (T) message : null);
          }
        }
      }

      private class EndState implements Runnable {

        public void run() {
          state = read;
          state.run();
        }
      }
    }
  }
}
