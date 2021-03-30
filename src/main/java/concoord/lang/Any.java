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
import concoord.concurrent.CancelException;
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

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final List<Awaitable<? extends T>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private final ArrayList<AnyAwaiter> awaiters;
    private State<T> controlState = new InputState();
    private int stoppedCount;

    private AnyControl(@NotNull Scheduler scheduler,
        @NotNull List<Awaitable<? extends T>> awaitables) {
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
      this.awaiters = new ArrayList<AnyAwaiter>(awaitables.size());
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      for (final Cancelable cancelable : cancelables) {
        cancelable.cancel();
      }
    }

    public void abortExecution(@NotNull Throwable error) {
      for (final Awaitable<?> awaitable : awaitables) {
        awaitable.abort();
      }
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class AnyAwaiter implements Awaiter<Object> {

      private final MessageCommand messageCommand = new MessageCommand();
      private final int index;
      private State<Object> awaiterState = new ReadState();
      private AwaitableFlowControl<?> flowControl;
      private int eventCount;

      private AnyAwaiter(int index) {
        this.index = index;
      }

      public void message(Object message) {
        queue.offer(message != null ? message : NULL);
        scheduler.scheduleLow(messageCommand);
      }

      public void error(@NotNull Throwable error) {
        if (error instanceof CancelException) {
          scheduler.scheduleLow(new CancelCommand());
        } else {
          scheduler.scheduleLow(new ErrorCommand(error));
        }
      }

      public void end() {
        scheduler.scheduleLow(new EndCommand());
      }

      @SuppressWarnings("unchecked")
      private boolean executeBlock(@NotNull AwaitableFlowControl<?> flowControl)
          throws Exception {
        this.flowControl = flowControl;
        return awaiterState.executeBlock((AwaitableFlowControl<Object>) flowControl);
      }

      private class MessageCommand implements Runnable {

        public void run() {
          if (eventCount > 0) {
            --eventCount;
          }
          flowControl.execute();
        }
      }

      private class CancelCommand implements Runnable {

        public void run() {
          eventCount = 0;
          flowControl.execute();
        }
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          awaiterState = new ErrorState(error);
          flowControl.execute();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          if (++stoppedCount == awaitables.size()) {
            awaiterState = new EndState();
            flowControl.execute();
          }
        }
      }

      private class ReadState implements State<Object> {

        public boolean executeBlock(@NotNull AwaitableFlowControl<Object> flowControl) {
          if (eventCount == 0) {
            eventCount = flowControl.outputEvents();
            cancelables.set(index, awaitables.get(index).await(eventCount, AnyAwaiter.this));
            return false;
          }
          return true;
        }
      }

      private class ErrorState implements State<Object> {

        private final Throwable error;

        private ErrorState(@NotNull Throwable error) {
          this.error = error;
        }

        public boolean executeBlock(@NotNull AwaitableFlowControl<Object> flowControl) {
          flowControl.error(error);
          cancelExecution(); // TODO: 18/03/21 ???
          return true;
        }
      }

      private class EndState implements State<Object> {

        public boolean executeBlock(@NotNull AwaitableFlowControl<Object> flowControl) {
          flowControl.stop();
          return true;
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        controlState = new MessageState();
        final List<Awaitable<? extends T>> awaitables = AnyControl.this.awaitables;
        final ArrayList<Cancelable> cancelables = AnyControl.this.cancelables;
        final ArrayList<AnyAwaiter> awaiters = AnyControl.this.awaiters;
        for (int i = 0; i < awaitables.size(); ++i) {
          final AnyAwaiter awaiter = new AnyAwaiter(i);
          cancelables.add(null);
          awaiters.add(awaiter);
          awaiter.executeBlock(flowControl);
        }
        return false;
      }
    }

    private class MessageState implements State<T> {

      @SuppressWarnings("unchecked")
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        final Object message = queue.poll();
        if (message != null) {
          flowControl.postOutput(message != NULL ? (T) message : null);
          return true;
        }
        boolean execute = false;
        final ArrayList<AnyAwaiter> awaiters = AnyControl.this.awaiters;
        for (final AnyAwaiter awaiter : awaiters) {
          if (awaiter.executeBlock(flowControl)) {
            execute = true;
          }
        }
        return execute;
      }
    }
  }
}
