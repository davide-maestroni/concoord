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
import concoord.flow.Result;
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

public class Do<T> implements Task<T> {

  private static final Object NULL = new Object();

  private final Block<T> block;

  public Do(@NotNull Block<T> block) {
    new IfNull(block, "block").throwException();
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new DoAwaitable<T>(scheduler, block);
  }

  public interface Block<T> {

    @NotNull
    Result<T> call() throws Exception;
  }

  private static class DoAwaitable<T> implements Awaitable<T> {

    private final Logger doLogger = new Logger(Awaitable.class, this);
    private final CircularQueue<DoFlowControl> flowControls = new CircularQueue<DoFlowControl>();
    private final CircularQueue<Awaitable<? extends T>> outputs = new CircularQueue<Awaitable<? extends T>>();
    private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final Block<T> block;
    private DoFlowControl currentFlowControl;
    private boolean stopped;

    private DoAwaitable(@NotNull Scheduler scheduler, @NotNull Block<T> block) {
      this.scheduler = scheduler;
      this.block = block;
      doLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
    }

    public void await(int maxEvents) {
      await(maxEvents, new DummyAwaiter<T>());
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      new IfNull(awaiter, "awaiter").throwException();
      scheduler.scheduleLow(new DoFlowControl(maxEvents, awaiter));
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
          doLogger.log(new InfMessage("[aborted]"));
        }
      });
    }

    private void nextFlowControl() {
      currentFlowControl = flowControls.poll();
      if (currentFlowControl != null) {
        scheduler.scheduleLow(currentFlowControl);
      } else {
        doLogger.log(new InfMessage("[settled]"));
      }
    }

    private class DoFlowControl implements FlowControl<T>, Awaiter<T>, Runnable {

      private final Logger flowLogger = new Logger(FlowControl.class, this);
      private final ReadState read = new ReadState();
      private final WriteState write = new WriteState();
      private final EndState end = new EndState();
      private final Awaiter<? super T> awaiter;
      private final int totEvents;
      private int events;
      private Runnable state;
      private int posts;

      private DoFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
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
        flowLogger.log(new DbgMessage("[limiting] event number ignored: %d", maxEvents));
      }

      public void stop() {
        stopped = true;
        flowLogger.log(new DbgMessage("[stopped]"));
        doLogger.log(new InfMessage("[complete]"));
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

      private void sendError(@NotNull Throwable error) {
        stopped = true;
        outputs.clear();
        try {
          awaiter.error(error);
        } catch (final Exception e) {
          doLogger.log(
              new ErrMessage(
                  new LogMessage(
                      "failed to notify error to awaiter: %s",
                      new PrintIdentity(awaiter)
                  ),
                  e
              )
          );
        }
        doLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
      }

      private void sendEnd() {
        try {
          awaiter.end();
        } catch (final Exception e) {
          doLogger.log(
              new ErrMessage(
                  new LogMessage("failed to notify end to awaiter: %s", new PrintIdentity(awaiter)),
                  e
              )
          );
          sendError(e);
        }
        doLogger.log(new InfMessage("[ended]"));
      }

      private class InitState implements Runnable {

        public void run() {
          final DoFlowControl thisFlowControl = DoFlowControl.this;
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
            final DoFlowControl flowControl = DoFlowControl.this;
            final CircularQueue<Awaitable<? extends T>> outputs = DoAwaitable.this.outputs;
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
                flowLogger.log(new DbgMessage("[invoking] block: %s", new PrintIdentity(block)));
                final Result<T> result = block.call();
                posts = 0;
                result.apply(flowControl);
                awaitable = outputs.poll();
                if (awaitable != null) {
                  state = write;
                  flowLogger.log(new DbgMessage("[writing]"));
                  awaitable.await(events, flowControl);
                  return;
                }
              } catch (final Exception e) {
                doLogger.log(
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
}
