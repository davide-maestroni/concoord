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

    private final InputState inputState = new InputState();
    private final MessageState messageState = new MessageState();
    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final List<Awaitable<? extends T>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private State<T> currentState = inputState;
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
      return currentState.executeBlock(flowControl);
    }

    public void abortExecution(@NotNull Throwable error) {
      for (final Awaitable<?> awaitable : awaitables) {
        awaitable.abort();
      }
    }

    public void cancelExecution() {
      for (Cancelable cancelable : cancelables) {
        cancelable.cancel();
      }
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class AnyAwaiter implements Awaiter<Object> {

      private final FlowCommand flowCmd = new FlowCommand();
      private final AwaitableFlowControl<T> flowControl;

      private AnyAwaiter(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void message(Object message) {
        inputs.offer(message != null ? message : NULL);
        scheduler.scheduleLow(flowCmd);
      }

      public void error(@NotNull Throwable error) {
        // TODO: 29/03/21 CancelException
        scheduler.scheduleLow(new ErrorCommand(error));
      }

      public void end() {
        scheduler.scheduleLow(new EndCommand());
      }

      private class FlowCommand implements Runnable {

        public void run() {
          flowControl.execute();
        }
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          inputs.offer(STOP);
          currentState = new ErrorState(error);
          flowControl.execute();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          if (++stopped == awaitables.size()) {
            inputs.offer(STOP);
            currentState = new EndState();
            flowControl.execute();
          }
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        currentState = messageState;
        events = maxEvents = flowControl.inputEvents();
        if (events < 0) {
          events = 1;
        }
        final ArrayList<Cancelable> cancelables = AnyControl.this.cancelables;
        for (final Awaitable<? extends T> awaitable : awaitables) {
          cancelables.add(awaitable.await(maxEvents, new AnyAwaiter(flowControl)));
        }
        return false;
      }
    }

    private class MessageState implements State<T> {

      @SuppressWarnings("unchecked")
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        final Object message = inputs.poll();
        if (message != null) {
          flowControl.postOutput(message != NULL ? (T) message : null);
          if (maxEvents >= 0) {
            --events;
          }
          return true;
        }
        if (events < 1) {
          // TODO: 29/03/21 broken
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
    }

    private class ErrorState extends MessageState {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        if (inputs.peek() == STOP) {
          flowControl.error(error);
          cancelExecution(); // TODO: 18/03/21 ???
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        if (inputs.peek() == STOP) {
          flowControl.stop();
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }
  }
}
