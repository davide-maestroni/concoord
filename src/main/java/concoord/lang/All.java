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
    return new BaseAwaitable<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(scheduler,
        new AllControl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(scheduler, awaitables));
  }

  private static class AllControl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>
      implements ExecutionControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final ArrayList<ConcurrentLinkedQueue<Object>> inputs;
    private final Scheduler scheduler;
    private final List<Awaitable<?>> awaitables;
    private final ArrayList<Cancelable> cancelables;
    private State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> state = input;
    private int maxEvents;
    private int events;

    private AllControl(@NotNull Scheduler scheduler, @NotNull List<Awaitable<?>> awaitables) {
      this.scheduler = scheduler;
      this.awaitables = awaitables;
      this.cancelables = new ArrayList<Cancelable>(awaitables.size());
      this.inputs = new ArrayList<ConcurrentLinkedQueue<Object>>(awaitables.size());
      for (final Awaitable<?> ignored : awaitables) {
        final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
        inputs.add(queue);
      }
    }

    public boolean executeBlock(
        @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl)
        throws Exception {
      return state.executeBlock(flowControl);
    }

    public void abortExecution() {
      for (final Awaitable<?> awaitable : awaitables) {
        awaitable.abort();
      }
    }

    private void cancelExecution() {
      for (Cancelable cancelable : cancelables) {
        cancelable.cancel();
      }
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class AllAwaiter implements Awaiter<Object> {

      private final AwaitableFlowControl<?> flowControl;
      private final ConcurrentLinkedQueue<Object> queue;

      private AllAwaiter(@NotNull AwaitableFlowControl<?> flowControl,
          @NotNull ConcurrentLinkedQueue<Object> queue) {
        this.flowControl = flowControl;
        this.queue = queue;
      }

      public void message(Object message) {
        queue.offer(message != null ? message : NULL);
        flowControl.schedule();
      }

      public void error(@NotNull Throwable error) {
        scheduler.scheduleLow(new ErrorCommand(error));
      }

      public void end() {
        scheduler.scheduleLow(new EndCommand());
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          queue.offer(STOP);
          state = new ErrorState(error);
          flowControl.schedule();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          queue.offer(STOP);
          state = new EndState();
          flowControl.schedule();
        }
      }
    }

    private class InputState implements State<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> {

      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl) {
        state = message;
        events = maxEvents = flowControl.outputEvents();
        final List<Awaitable<?>> awaitables = AllControl.this.awaitables;
        for (int i = 0; i < awaitables.size(); ++i) {
          cancelables.add(
              awaitables.get(i).await(events, new AllAwaiter(flowControl, inputs.get(i)))
          );
        }
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
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
          flowControl.logger().log(new DbgMessage("[executing] message zipping"));
          flowControl.postOutput(new Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>(messages));
          if (maxEvents >= 0) {
            --events;
          }
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          final List<Awaitable<?>> awaitables = AllControl.this.awaitables;
          final ArrayList<Cancelable> cancelables = AllControl.this.cancelables;
          cancelables.clear();
          for (int i = 0; i < awaitables.size(); ++i) {
            cancelables
                .add(awaitables.get(0).await(events, new AllAwaiter(flowControl, inputs.get(i))));
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
      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl) {
        for (final ConcurrentLinkedQueue<Object> queue : inputs) {
          final Object message = queue.peek();
          if (message == STOP) {
            flowControl.error(error);
            cancelExecution(); // TODO: 18/03/21 ???
            return false;
          }
        }
        return super.executeBlock(flowControl);
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(
          @NotNull AwaitableFlowControl<Tuple<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> flowControl) {
        for (final ConcurrentLinkedQueue<Object> queue : inputs) {
          final Object message = queue.peek();
          if (message == STOP) {
            flowControl.stop();
            cancelExecution(); // TODO: 18/03/21 ???
            return true;
          }
        }
        return super.executeBlock(flowControl);
      }
    }
  }
}
