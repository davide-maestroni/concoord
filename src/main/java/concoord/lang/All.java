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
import concoord.logging.DbgMessage;
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
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
        new IfNull("first", first),
        new IfNull("second", second)
    ).throwException();
    awaitables = Arrays.asList(first, second);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third)
    ).throwException();
    awaitables = Arrays.asList(first, second, third);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth,
      @NotNull Awaitable<T5> fifth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth, fifth);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth, @NotNull Awaitable<T5> fifth,
      @NotNull Awaitable<T6> sixth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth),
        new IfNull("sixth", sixth)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth, fifth, sixth);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth, @NotNull Awaitable<T5> fifth,
      @NotNull Awaitable<T6> sixth, @NotNull Awaitable<T7> seventh) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth),
        new IfNull("sixth", sixth),
        new IfNull("seventh", seventh)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth, fifth, sixth, seventh);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth, @NotNull Awaitable<T5> fifth,
      @NotNull Awaitable<T6> sixth, @NotNull Awaitable<T7> seventh, @NotNull Awaitable<T8> eighth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth),
        new IfNull("sixth", sixth),
        new IfNull("seventh", seventh),
        new IfNull("eighth", eighth)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth, @NotNull Awaitable<T5> fifth,
      @NotNull Awaitable<T6> sixth, @NotNull Awaitable<T7> seventh, @NotNull Awaitable<T8> eighth,
      @NotNull Awaitable<T9> ninth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth),
        new IfNull("sixth", sixth),
        new IfNull("seventh", seventh),
        new IfNull("eighth", eighth),
        new IfNull("ninth", ninth)
    ).throwException();
    awaitables = Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth);
  }

  public All(@NotNull Awaitable<T1> first, @NotNull Awaitable<T2> second,
      @NotNull Awaitable<T3> third, @NotNull Awaitable<T4> fourth, @NotNull Awaitable<T5> fifth,
      @NotNull Awaitable<T6> sixth, @NotNull Awaitable<T7> seventh, @NotNull Awaitable<T8> eighth,
      @NotNull Awaitable<T9> ninth, @NotNull Awaitable<T10> tenth) {
    new IfSomeOf(
        new IfNull("first", first),
        new IfNull("second", second),
        new IfNull("third", third),
        new IfNull("fourth", third),
        new IfNull("fifth", fifth),
        new IfNull("sixth", sixth),
        new IfNull("seventh", seventh),
        new IfNull("eighth", eighth),
        new IfNull("ninth", ninth),
        new IfNull("tenth", tenth)
    ).throwException();
    awaitables =
        Arrays.asList(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, tenth);
  }

  public All(@NotNull Awaitable<?>... awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", (Object[]) awaitables)
    ).throwException();
    this.awaitables = Arrays.asList(awaitables);
  }

  public All(@NotNull List<Awaitable<?>> awaitables) {
    new IfAnyOf(
        new IfNull("awaitables", awaitables),
        new IfContainsNull("awaitables", awaitables)
    ).throwException();
    this.awaitables = awaitables;
  }

  @NotNull
  public Awaitable<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> on(
      @NotNull Scheduler scheduler) {
    return new BaseAwaitable<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(
        scheduler,
        new AllControl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(scheduler, awaitables)
    );
  }

  private static class AllControl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
      implements ExecutionControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

    private static final Object NULL = new Object();

    private final Scheduler scheduler;
    private final List<Awaitable<?>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private final ArrayList<AllAwaiter> awaiters;
    private State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> controlState = new InputState();

    private AllControl(@NotNull Scheduler scheduler, @NotNull List<Awaitable<?>> awaitables) {
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
      this.awaiters = new ArrayList<AllAwaiter>(awaitables.size());
    }

    public boolean executeBlock(
        @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl)
        throws Exception {
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

    private class AllAwaiter implements Awaiter<Object> {

      private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
      private final MessageCommand messageCommand = new MessageCommand();
      private final int index;
      private State<Object> awaiterState = new ReadState();
      private AwaitableFlowControl<?> flowControl;
      private int eventCount;

      private AllAwaiter(int index) {
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

      @NotNull
      private ConcurrentLinkedQueue<Object> queue() {
        return queue;
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
          awaiterState = new EndState();
          flowControl.execute();
        }
      }

      private class ReadState implements State<Object> {

        public boolean executeBlock(@NotNull AwaitableFlowControl<Object> flowControl) {
          if (eventCount == 0) {
            eventCount = flowControl.outputEvents();
            cancelables.set(index, awaitables.get(index).await(eventCount, AllAwaiter.this));
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

        public boolean executeBlock(@NotNull AwaitableFlowControl<Object> flowControl)
            throws Exception {
          flowControl.stop();
          cancelExecution(); // TODO: 18/03/21 ???
          return true;
        }
      }
    }

    private class InputState implements State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl)
          throws Exception {
        controlState = new MessageState();
        final List<Awaitable<?>> awaitables = AllControl.this.awaitables;
        final ArrayList<Cancelable> cancelables = AllControl.this.cancelables;
        final ArrayList<AllAwaiter> awaiters = AllControl.this.awaiters;
        for (int i = 0; i < awaitables.size(); ++i) {
          final AllAwaiter awaiter = new AllAwaiter(i);
          cancelables.add(null);
          awaiters.add(awaiter);
          awaiter.executeBlock(flowControl);
        }
        return false;
      }
    }

    private class MessageState implements State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl)
          throws Exception {
        boolean execute = false;
        final ArrayList<Object> messages = new ArrayList<Object>();
        final ArrayList<AllAwaiter> awaiters = AllControl.this.awaiters;
        for (final AllAwaiter awaiter : awaiters) {
          final Object message = awaiter.queue().peek();
          if (message != null) {
            messages.add(message != NULL ? message : null);
          } else if (awaiter.executeBlock(flowControl)) {
            execute = true;
          }
        }
        if (messages.size() == awaiters.size()) {
          for (final AllAwaiter awaiter : awaiters) {
            awaiter.queue().poll();
          }
          flowControl.logger().log(new DbgMessage("[executing] message zipping"));
          flowControl.postOutput(new Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(messages));
          return true;
        }
        return execute;
      }
    }
  }
}
