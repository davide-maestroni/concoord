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
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
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
    return new BaseAwaitable<T>(scheduler, new AnyControl<T>(scheduler, awaitables));
  }

  private static class AnyControl<T> implements ExecutionControl<T> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final List<Awaitable<? extends T>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private State<T> state = input;
    private int maxEvents;
    private int events;
    private int stopped;

    private AnyControl(@NotNull Scheduler scheduler,
        @NotNull List<Awaitable<? extends T>> awaitables) {
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    public void cancelExecution() {
      state.cancelExecution();
    }

    public void abortExecution() {
      for (final Awaitable<?> awaitable : awaitables) {
        awaitable.abort();
      }
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;

      void cancelExecution();
    }

    private class AnyAwaiter implements Awaiter<Object> {

      private final AwaitableFlowControl<T> flowControl;

      private AnyAwaiter(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void message(Object message) {
        inputs.offer(message != null ? message : NULL);
        flowControl.schedule();
      }

      public void error(@NotNull Throwable error) {
        scheduler.scheduleLow(new ErrorCommand(error));
      }

      public void end(int reason) {
        scheduler.scheduleLow(new EndCommand());
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          inputs.offer(STOP);
          state = new ErrorState(error);
          flowControl.schedule();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          if (++stopped == awaitables.size()) {
            inputs.offer(STOP);
            state = new EndState();
            flowControl.schedule();
          }
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        state = message;
        events = maxEvents = flowControl.outputEvents();
        final ArrayList<Cancelable> cancelables = AnyControl.this.cancelables;
        for (final Awaitable<? extends T> awaitable : awaitables) {
          cancelables.add(awaitable.await(events, new AnyAwaiter(flowControl)));
        }
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }

      public void cancelExecution() {
        for (final Cancelable cancelable : cancelables) {
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
          final ArrayList<Cancelable> cancelables = AnyControl.this.cancelables;
          cancelables.clear();
          stopped = 0;
          for (final Awaitable<? extends T> awaitable : awaitables) {
            cancelables.add(awaitable.await(events, new AnyAwaiter(flowControl)));
          }
        }
        return false;
      }

      public void cancelExecution() {
        final ArrayList<Cancelable> cancelables = AnyControl.this.cancelables;
        for (final Cancelable cancelable : cancelables) {
          cancelable.cancel();
        }
        cancelables.clear();
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
    }
  }
}
