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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
import concoord.util.assertion.IfNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Any<T> implements Task<T> {

  private final List<Awaitable<? extends T>> awaitables;

  public Any(@NotNull Awaitable<? extends T>... awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", (Object[]) awaitables)
    ).throwException();
    this.awaitables = Arrays.asList(awaitables);
  }

  public Any(@NotNull List<Awaitable<? extends T>> awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", awaitables)
    ).throwException();
    this.awaitables = awaitables;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull("scheduler", scheduler).throwException();
    return new AnyAwaitable<T>(scheduler, awaitables);
  }

  private static class AnyAwaitable<T> extends BaseAwaitable<T> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final List<Awaitable<? extends T>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private final ArrayList<AnyAwaiter> awaiters;
    private State<T> state = input;
    private int maxEvents;
    private int events;
    private int stopped;

    private AnyAwaitable(@NotNull Scheduler scheduler,
        @NotNull List<Awaitable<? extends T>> awaitables) {
      super(scheduler);
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
      this.awaiters = new ArrayList<AnyAwaiter>(awaitables.size());
      for (final Awaitable<?> ignored : awaitables) {
        awaiters.add(new AnyAwaiter());
      }
    }

    @Override
    protected boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    @Override
    protected void cancelExecution() {
      state.cancelExecution();
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;

      void cancelExecution();
    }

    private class AnyAwaiter implements Awaiter<Object> {

      public void message(Object message) {
        inputs.offer(message != null ? message : NULL);
        scheduleFlow();
      }

      public void error(@NotNull final Throwable error) {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            inputs.offer(STOP);
            state = new ErrorState(error);
            scheduleFlow();
          }
        });
      }

      public void end() {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            if (++stopped == awaitables.size()) {
              inputs.offer(STOP);
              state = new EndState();
              scheduleFlow();
            }
          }
        });
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        state = message;
        events = maxEvents = flowControl.outputEvents();
        final List<Awaitable<? extends T>> awaitables = AnyAwaitable.this.awaitables;
        for (int i = 0; i < awaitables.size(); ++i) {
          cancelables.add(awaitables.get(i).await(events, awaiters.get(i)));
        }
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }

      public void cancelExecution() {
        for (Cancelable cancelable : cancelables) {
          cancelable.cancel();
        }
      }
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        final Object message = inputs.poll();
        if (message != null) {
          postMessage(flowControl, message);
          if (maxEvents >= 0) {
            --events;
          }
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          final List<Awaitable<? extends T>> awaitables = AnyAwaitable.this.awaitables;
          final ArrayList<Cancelable> cancelables = AnyAwaitable.this.cancelables;
          cancelables.clear();
          for (int i = 0; i < awaitables.size(); ++i) {
            cancelables.add(awaitables.get(0).await(events, awaiters.get(i)));
          }
        }
        return false;
      }

      public void cancelExecution() {
        for (Cancelable cancelable : cancelables) {
          cancelable.cancel();
        }
      }

      @SuppressWarnings("unchecked")
      void postMessage(@NotNull AwaitableFlowControl<T> flowControl, Object message) {
        flowControl.postOutput(message != NULL ? (T) message : null);
      }
    }

    private class ErrorState extends MessageState {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      @Override
      void postMessage(@NotNull AwaitableFlowControl<T> flowControl, Object message) {
        if (message == STOP) {
          flowControl.abort(error);
        } else {
          super.postMessage(flowControl, message);
        }
      }

      @Override
      public void cancelExecution() {
        for (Cancelable cancelable : cancelables) {
          cancelable.cancel();
        }
      }
    }

    private class EndState extends MessageState {

      @Override
      void postMessage(@NotNull AwaitableFlowControl<T> flowControl, Object message) {
        if (message == STOP) {
          flowControl.stop();
        } else {
          super.postMessage(flowControl, message);
        }
      }

      @Override
      public void cancelExecution() {
        for (Cancelable cancelable : cancelables) {
          cancelable.cancel();
        }
      }
    }
  }
}
