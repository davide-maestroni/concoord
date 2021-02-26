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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public abstract class BaseAwaitable<T> implements Awaitable<T> {

  private static final Object NULL = new Object();

  private final Logger awaitableLogger = new Logger(Awaitable.class, this);
  private final CircularQueue<InternalFlowControl> flowControls = new CircularQueue<InternalFlowControl>();
  private final CircularQueue<Awaitable<? extends T>> outputs = new CircularQueue<Awaitable<? extends T>>();
  private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
  private final Scheduler scheduler;
  private InternalFlowControl currentFlowControl;
  private boolean stopped;

  public BaseAwaitable(@NotNull Scheduler scheduler) {
    this.scheduler = scheduler;
    awaitableLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
  }

  public void await(int maxEvents) {
    await(maxEvents, new DummyAwaiter<T>());
  }

  public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
    new IfNull(awaiter, "awaiter").throwException();
    scheduler.scheduleLow(new InternalFlowControl(maxEvents, awaiter));
  }

  public void await(int maxEvents, @NotNull UnaryAwaiter<? super T> messageAwaiter,
      @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter) {
    await(maxEvents, new CombinedAwaiter<T>(messageAwaiter, errorAwaiter, endAwaiter));
  }

  public void abort() {
    scheduler.scheduleHigh(new Runnable() {
      public void run() {
        stopped = true;
        outputs.clear();
        awaitableLogger.log(new InfMessage("[aborted]")); // TODO: 25/02/21 FIX IT!!
      }
    });
  }

  protected abstract boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl)
      throws Exception;

  private void nextFlowControl() {
    currentFlowControl = flowControls.poll();
    if (currentFlowControl != null) {
      scheduler.scheduleLow(currentFlowControl);
    } else {
      awaitableLogger.log(new InfMessage("[settled]"));
    }
  }

  protected interface AwaitableFlowControl<T> extends FlowControl<T>, Awaiter<T>, Runnable {

    int inputEvents();

    @NotNull
    Logger logger();
  }

  private static class DummyAwaiter<T> implements Awaiter<T> {

    public void message(T message) {
    }

    public void error(@NotNull Throwable error) {
    }

    public void end() {
    }
  }

  private class InternalFlowControl implements AwaitableFlowControl<T> {

    private final Logger flowLogger = new Logger(FlowControl.class, this);
    private final ReadState read = new ReadState();
    private final WriteState write = new WriteState();
    private final EndState end = new EndState();
    private final Awaiter<? super T> awaiter;
    private final int totEvents;
    private int inputEvents = 1;
    private int events;
    private Runnable state;
    private int posts;

    private InternalFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      this.totEvents = maxEvents;
      this.events = maxEvents;
      this.awaiter = awaiter;
      this.state = new InitState();
      flowLogger.log(new DbgMessage("[initialized]"));
    }

    public void postOutput(T message) {
      if (++posts > 1) {
        throw new IllegalStateException("multiple outputs posted by the result");
      }
      if (totEvents < Integer.MAX_VALUE) {
        flowLogger.log(new DbgMessage("[posting] new message: %d", totEvents - events));
        --events;
      } else {
        flowLogger.log(new DbgMessage("[posting] new message"));
      }
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
        flowLogger.log(
            new DbgMessage("[posting] new awaitable: %s", new PrintIdentity(awaitable))
        );
        outputs.offer(awaitable);
      } else {
        flowLogger.log(new WrnMessage("[posting] awaitable ignored: null"));
      }
    }

    public void nextInputs(int maxEvents) {
      flowLogger.log(new DbgMessage("[limiting] event number: %d", maxEvents));
      inputEvents = maxEvents;
    }

    public void stop() {
      stopped = true;
      flowLogger.log(new DbgMessage("[stopped]"));
      awaitableLogger.log(new InfMessage("[complete]"));
    }

    public void message(T message) {
      messages.offer(message != null ? message : NULL);
      scheduler.scheduleLow(this);
    }

    public void error(@NotNull final Throwable error) {
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

    public int inputEvents() {
      return inputEvents;
    }

    @NotNull
    public Logger logger() {
      return flowLogger;
    }

    private void sendError(@NotNull Throwable error) {
      stopped = true;
      outputs.clear();
      try {
        awaiter.error(error);
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(
                new LogMessage(
                    "failed to notify error to awaiter: %s",
                    new PrintIdentity(awaiter)
                ),
                e
            )
        );
      }
      awaitableLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
    }

    private void sendEnd() {
      try {
        awaiter.end();
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(
                new LogMessage("failed to notify end to awaiter: %s", new PrintIdentity(awaiter)),
                e
            )
        );
        sendError(e);
      }
      awaitableLogger.log(new InfMessage("[ended]"));
    }

    private class InitState implements Runnable {

      public void run() {
        final InternalFlowControl thisFlowControl = InternalFlowControl.this;
        if (currentFlowControl == null) {
          currentFlowControl = thisFlowControl;
        }
        if (currentFlowControl != thisFlowControl) {
          flowControls.offer(thisFlowControl);
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
        } else {
          final InternalFlowControl flowControl = InternalFlowControl.this;
          final CircularQueue<Awaitable<? extends T>> outputs = BaseAwaitable.this.outputs;
          Awaitable<? extends T> awaitable = outputs.poll();
          if (awaitable != null) {
            state = write;
            flowLogger.log(new DbgMessage("[writing]"));
            awaitable.await(events, flowControl);
          } else if (stopped) {
            sendEnd();
            nextFlowControl();
          } else {
            try {
              posts = 0;
              if (!executeBlock(flowControl)) {
                return;
              }
            } catch (final Exception e) {
              awaitableLogger.log(
                  new WrnMessage(new LogMessage("invocation failed with an exception"), e)
              );
              sendError(e);
              events = 0;
            }
            scheduler.scheduleLow(flowControl);
          }
        }
      }
    }

    private class WriteState implements Runnable {

      @SuppressWarnings("unchecked")
      public void run() {
        final Object message = messages.poll();
        if (message != null) {
          posts = 0;
          postOutput(message != NULL ? (T) message : null);
        }
      }
    }

    private class EndState implements Runnable {

      public void run() {
        state = read;
        flowLogger.log(new DbgMessage("[reading]"));
        state.run();
      }
    }
  }
}
