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
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import concoord.util.collection.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class All<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
    implements Task<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

  private final List<Awaitable<?>> awaitables;

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second) {
    new IfSomeOf(
        new IfNull(first, "first"),
        new IfNull(second, "second")
    ).throwException();
    awaitables = Arrays.asList(first, second);
  }

  @NotNull
  public Awaitable<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new AllAwaitable<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(scheduler, awaitables);
  }

  private static class AllAwaitable<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
      extends BaseAwaitable<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final ArrayList<ConcurrentLinkedQueue<Object>> inputs;
    private final Scheduler scheduler;
    private final List<Awaitable<?>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private final ArrayList<AllAwaiter> awaiters;
    private State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> state = input;
    private int maxEvents;
    private int events;

    private AllAwaitable(@NotNull Scheduler scheduler, @NotNull List<Awaitable<?>> awaitables) {
      super(scheduler);
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
      this.inputs = new ArrayList<ConcurrentLinkedQueue<Object>>(awaitables.size());
      this.awaiters = new ArrayList<AllAwaiter>(awaitables.size());
      for (final Awaitable<?> ignored : awaitables) {
        final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
        inputs.add(queue);
        awaiters.add(new AllAwaiter(queue));
      }
    }

    @Override
    protected boolean executeBlock(
        @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl)
        throws Exception {
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

    private class AllAwaiter implements Awaiter<Object> {

      private final ConcurrentLinkedQueue<Object> queue;

      private AllAwaiter(@NotNull ConcurrentLinkedQueue<Object> queue) {
        this.queue = queue;
      }

      public void message(Object message) {
        queue.offer(message != null ? message : NULL);
        scheduleFlow();
      }

      public void error(@NotNull final Throwable error) {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            queue.offer(STOP);
            state = new ErrorState(error);
            scheduleFlow();
          }
        });
      }

      public void end() {
        scheduler.scheduleLow(new Runnable() {
          public void run() {
            queue.offer(STOP);
            state = new EndState();
            scheduleFlow();
          }
        });
      }
    }

    private class InputState implements State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl) {
        state = message;
        events = maxEvents = flowControl.outputEvents();
        final List<Awaitable<?>> awaitables = AllAwaitable.this.awaitables;
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

    private class MessageState implements State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl) {
        final ArrayList<Object> messages = new ArrayList<Object>();
        for (final ConcurrentLinkedQueue<Object> queue : inputs) {
          final Object message = queue.peek();
          if (message == null) {
            break;
          }
          messages.add(message != NULL ? message : null);
        }
        if (messages.size() == inputs.size()) {
          for (final ConcurrentLinkedQueue<Object> queue : inputs) {
            queue.poll();
          }
          postMessages(flowControl, messages);
          if (maxEvents >= 0) {
            --events;
          }
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          final List<Awaitable<?>> awaitables = AllAwaitable.this.awaitables;
          final ArrayList<Cancelable> cancelables = AllAwaitable.this.cancelables;
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

      void postMessages(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl,
          @NotNull final ArrayList<Object> messages) {
        flowControl.postOutput(new Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(messages));
      }
    }

    private class ErrorState extends MessageState {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      @Override
      void postMessages(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl,
          @NotNull ArrayList<Object> messages) {
        if (messages.contains(STOP)) {
          flowControl.abort(error);
        } else {
          super.postMessages(flowControl, messages);
        }
      }
    }

    private class EndState extends MessageState {

      @Override
      void postMessages(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl,
          @NotNull ArrayList<Object> messages) {
        if (messages.contains(STOP)) {
          flowControl.stop();
        } else {
          super.postMessages(flowControl, messages);
        }
      }
    }
  }
}
