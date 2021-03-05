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
import concoord.concurrent.Cancelable;
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
import concoord.util.assertion.IfInterrupt;
import concoord.util.assertion.IfNull;
import concoord.util.collection.CircularQueue;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public abstract class BaseAwaitable<T> implements Awaitable<T> {

  private static final Object NULL = new Object();

  private final Logger awaitableLogger = new Logger(Awaitable.class, this);
  private final CircularQueue<InternalFlowControl> flowControls = new CircularQueue<InternalFlowControl>();
  private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
  private final Scheduler scheduler;
  private InternalFlowControl currentFlowControl;
  private Awaitable<? extends T> awaitable;
  private Cancelable cancelable;
  private boolean hasOutputs;
  private boolean stopped;

  public BaseAwaitable(@NotNull Scheduler scheduler) {
    this.scheduler = scheduler;
    awaitableLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
  }

  @NotNull
  public Cancelable await(int maxEvents) {
    return await(maxEvents, new DummyAwaiter<T>());
  }

  @NotNull
  public Cancelable await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
    new IfNull(awaiter, "awaiter").throwException();
    InternalFlowControl flowControl = new InternalFlowControl(maxEvents, awaiter);
    scheduler.scheduleLow(flowControl);
    return new BaseCancelable(flowControl);
  }

  @NotNull
  public Cancelable await(int maxEvents, @NotNull UnaryAwaiter<? super T> messageAwaiter,
      @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter) {
    return await(maxEvents, new CombinedAwaiter<T>(messageAwaiter, errorAwaiter, endAwaiter));
  }

  public void abort() {
    scheduler.scheduleHigh(new Runnable() {
      public void run() {
        final InternalFlowControl flowControl = BaseAwaitable.this.currentFlowControl;
        try {
          cancelAwaitable();
          stopped = true;
          if (flowControl != null) {
            flowControl.state = new NoopState();
            if (flowControl.events != 0) {
              flowControl.sendEnd();
            }
          }
          awaitableLogger.log(new InfMessage("[aborted]"));
        } catch (final Exception e) {
          awaitableLogger.log(new ErrMessage(new LogMessage("failed to cancel execution"), e));
          new IfInterrupt(e).throwException();
          if ((flowControl != null) && (flowControl.events != 0)) {
            flowControl.sendError(e);
          }
        }
        nextFlowControl();
      }
    });
  }

  protected abstract boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl)
      throws Exception;

  protected abstract void cancelExecution() throws Exception;

  protected void scheduleFlow() {
    if (currentFlowControl != null) {
      scheduler.scheduleLow(currentFlowControl);
    }
  }

  private void nextFlowControl() {
    currentFlowControl = flowControls.poll();
    if (currentFlowControl != null) {
      scheduler.scheduleLow(currentFlowControl);
    } else {
      awaitableLogger.log(new InfMessage("[settled]"));
    }
  }

  private void cancelAwaitable() throws Exception {
    if (cancelable != null) {
      cancelable.cancel();
      awaitable = null;
      cancelable = null;
    }
    cancelExecution();
  }

  protected interface AwaitableFlowControl<T> extends FlowControl<T> {

    void abort(Throwable error);

    int inputEvents();

    boolean outputEvents();

    @NotNull
    Logger logger();
  }

  private static class BaseCancelable implements Cancelable {

    private final Cancelable cancelable;

    private BaseCancelable(@NotNull Cancelable cancelable) {
      this.cancelable = cancelable;
    }

    public boolean isError() {
      return cancelable.isError();
    }

    public boolean isDone() {
      return cancelable.isDone();
    }

    public void cancel() {
      cancelable.cancel();
    }
  }

  private static class DummyAwaiter<T> implements Awaiter<T> {

    public void message(T message) {
    }

    public void error(@NotNull Throwable error) {
    }

    public void end() {
    }
  }

  private static class NoopState implements Runnable {

    public void run() {
      // noop
    }
  }

  private class InternalFlowControl implements AwaitableFlowControl<T>, Awaiter<T>, Cancelable,
      Runnable {

    private static final int ERROR = -1;
    private static final int RUNNING = 0;
    private static final int DONE = 1;

    private final Logger flowLogger = new Logger(FlowControl.class, this);
    private final AtomicInteger status = new AtomicInteger();
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
      if (totEvents >= 0) {
        flowLogger.log(new DbgMessage("[posting] new message: %d", totEvents - events));
        --events;
      } else {
        flowLogger.log(new DbgMessage("[posting] new message"));
      }
      try {
        awaiter.message(message);
        hasOutputs = true;
      } catch (final Exception e) {
        new IfInterrupt(e).throwException();
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
        BaseAwaitable.this.awaitable = awaitable;
      } else {
        flowLogger.log(new WrnMessage("[posting] awaitable ignored: null"));
      }
    }

    public void nextInputs(int maxEvents) {
      flowLogger.log(new DbgMessage("[limiting] event number: %d", maxEvents));
      inputEvents = maxEvents;
    }

    public void stop() {
      try {
        cancelExecution();
        stopped = true;
        flowLogger.log(new DbgMessage("[stopped]"));
        awaitableLogger.log(new InfMessage("[complete]"));
      } catch (final Exception e) {
        awaitableLogger.log(new ErrMessage(new LogMessage("failed to cancel execution"), e));
        new IfInterrupt(e).throwException();
        if (events != 0) {
          sendError(e);
        }
        nextFlowControl();
      }
    }

    public void message(T message) {
      messages.offer(message != null ? message : NULL);
      scheduler.scheduleLow(this);
    }

    public void error(@NotNull final Throwable error) {
      scheduler.scheduleLow(new Runnable() {
        public void run() {
          error(error);
        }
      });
    }

    public void end() {
      scheduler.scheduleLow(end);
    }

    public void abort(Throwable error) {
      sendError(error);
      nextFlowControl();
    }

    public int inputEvents() {
      return inputEvents;
    }

    public boolean outputEvents() {
      return hasOutputs;
    }

    @NotNull
    public Logger logger() {
      return flowLogger;
    }

    public boolean isError() {
      return status.get() == ERROR;
    }

    public boolean isDone() {
      return status.get() == DONE;
    }

    public void cancel() {
      if (status.get() != RUNNING) {
        return;
      }
      scheduler.scheduleHigh(new Runnable() {
        public void run() {
          final Iterator<InternalFlowControl> iterator = flowControls.iterator();
          while (iterator.hasNext()) {
            InternalFlowControl flowControl = iterator.next();
            if (flowControl.awaiter.equals(awaiter)) {
              iterator.remove();
            }
          }
          if (currentFlowControl == InternalFlowControl.this) {
            try {
              cancelAwaitable();
              state = new NoopState();
            } catch (final Exception e) {
              awaitableLogger.log(new ErrMessage(new LogMessage("failed to cancel execution"), e));
              new IfInterrupt(e).throwException();
              if (events != 0) {
                sendError(e);
              }
            }
            nextFlowControl();
          }
        }
      });
    }

    public void run() {
      state.run();
    }

    private void sendError(@NotNull Throwable error) {
      stopped = true;
      awaitable = null;
      state = new NoopState();
      awaitableLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
      try {
        awaiter.error(error);
        status.set(ERROR);
        hasOutputs = true;
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
        new IfInterrupt(e).throwException();
      }
    }

    private void sendEnd() {
      awaitableLogger.log(new InfMessage("[ended]"));
      try {
        awaiter.end();
        status.set(DONE);
        hasOutputs = true;
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(
                new LogMessage("failed to notify end to awaiter: %s", new PrintIdentity(awaiter)),
                e
            )
        );
        new IfInterrupt(e).throwException();
        sendError(e);
      }
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
          if (events != 0) {
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
        final InternalFlowControl flowControl = InternalFlowControl.this;
        Awaitable<? extends T> awaitable = BaseAwaitable.this.awaitable;
        if (awaitable != null) {
          state = write;
          flowLogger.log(new DbgMessage("[writing]"));
          cancelable = awaitable.await(events, flowControl);
        } else if (stopped) {
          sendEnd();
          nextFlowControl();
        } else {
          try {
            posts = 0;
            if (!executeBlock(flowControl)) {
              return;
            }
            if (events == 0) {
              nextFlowControl();
              return;
            }
          } catch (final Exception e) {
            awaitableLogger.log(
                new WrnMessage(new LogMessage("invocation failed with an exception"), e)
            );
            new IfInterrupt(e).throwException();
            sendError(e);
            nextFlowControl();
            return;
          }
          scheduler.scheduleLow(flowControl);
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
          if (events == 0) {
            nextFlowControl();
          }
        }
      }
    }

    private class EndState implements Runnable {

      public void run() {
        awaitable = null;
        state = read;
        flowLogger.log(new DbgMessage("[reading]"));
        state.run();
      }
    }
  }
}
