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
import concoord.util.assertion.IfSomeOf;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class For<T, M> implements Task<T> {

  private static final Object NULL = new Object();

  private final Awaitable<M> awaitable;
  private final Block<T, ? super M> block;

  public For(@NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
    new IfSomeOf(
        new IfNull(block, "awaitable"),
        new IfNull(block, "block")
    ).throwException();
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new ForAwaitable<T, M>(scheduler, awaitable, block);
  }

  public interface Block<T, M> {

    @NotNull
    Result<T> call(M message) throws Exception;
  }

  private static class ForAwaitable<T, M> implements Awaitable<T> {

    private final Logger doLogger = new Logger(Awaitable.class, this);
    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final CircularQueue<ForFlowControl> flowControls = new CircularQueue<ForFlowControl>();
    private final CircularQueue<Awaitable<? extends T>> outputs = new CircularQueue<Awaitable<? extends T>>();
    private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final Awaitable<M> awaitable;
    private final Block<T, ? super M> block;
    private ForFlowControl currentFlowControl;
    private boolean stopped;

    private ForAwaitable(@NotNull Scheduler scheduler, @NotNull Awaitable<M> awaitable,
        @NotNull Block<T, ? super M> block) {
      this.scheduler = scheduler;
      this.awaitable = awaitable;
      this.block = block;
      doLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
    }

    public void await(int maxEvents) {
      await(maxEvents, new DummyAwaiter<T>());
    }

    public void await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
      new IfNull(awaiter, "awaiter").throwException();
      scheduler.scheduleLow(new ForFlowControl(maxEvents, awaiter));
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

    private class ForFlowControl implements FlowControl<T>, Awaiter<T>, Runnable {

      private final Logger flowLogger = new Logger(FlowControl.class, this);
      private final InputState input = new InputState();
      private final ReadState read = new ReadState();
      private final WriteState write = new WriteState();
      private final EndState end = new EndState();
      private final Awaiter<? super T> awaiter;
      private final int totEvents;
      private int inputEvents = 1;
      private int events;
      private Runnable state;
      private int posts;

      private ForFlowControl(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
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
          final ForFlowControl thisFlowControl = ForFlowControl.this;
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
            input.run();
          }
        }
      }

      private class InputState implements Runnable, Awaiter<M> {

        public void run() {
          awaitable.await(Math.max(1, inputEvents), this);
        }

        public void message(M message) throws Exception {
          inputs.offer(message != null ? message : NULL);
          scheduler.scheduleLow(read);
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
          scheduler.scheduleLow(read);
        }
      }

      private class ReadState implements Runnable {

        @SuppressWarnings("unchecked")
        public void run() {
          if (events < 1) {
            nextFlowControl();
          } else {
            final ForFlowControl currentFlowControl = ForAwaitable.this.currentFlowControl;
            final CircularQueue<Awaitable<? extends T>> outputs = ForAwaitable.this.outputs;
            Awaitable<? extends T> awaitable = outputs.poll();
            if (awaitable != null) {
              state = write;
              flowLogger.log(new DbgMessage("[writing]"));
              awaitable.await(events, currentFlowControl);
            } else if (stopped) {
              sendEnd();
              nextFlowControl();
            } else if (inputs.isEmpty()) {
              scheduler.scheduleLow(input);
            } else {
              try {
                flowLogger.log(new DbgMessage("[invoking] block: %s", new PrintIdentity(block)));
                final Object message = inputs.poll();
                final Result<T> result = block.call(message != NULL ? (M) message : null);
                posts = 0;
                result.apply(currentFlowControl);
                awaitable = outputs.poll();
                if (awaitable != null) {
                  state = write;
                  flowLogger.log(new DbgMessage("[writing]"));
                  awaitable.await(events, currentFlowControl);
                  return;
                }
              } catch (final Exception e) {
                doLogger.log(
                    new WrnMessage(new LogMessage("invocation failed with an exception"), e)
                );
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
