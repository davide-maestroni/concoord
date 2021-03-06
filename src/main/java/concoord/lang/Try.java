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
import concoord.flow.Continue;
import concoord.flow.Result;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Try<T> implements Task<T> {

  private final Awaitable<T> awaitable;

  public Try(@NotNull Awaitable<T> awaitable) {
    new IfNull("awaitable", awaitable).throwException();
    this.awaitable = awaitable;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull("scheduler", scheduler).throwException();
    return new TryAwaitable<T>(scheduler, awaitable);
  }

  public interface Block<T> {

    @NotNull
    Result<T> execute(@NotNull Throwable throwable) throws Exception;
  }

  public static class Catch<T> implements Block<T> {

    private final List<Class<? extends Throwable>> types;
    private final Block<T> block;

    public Catch(@NotNull Class<? extends Throwable> first, @NotNull Block<T> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("block", block)
      ).throwException();
      this.types = Collections.<Class<? extends Throwable>>singletonList(first);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends Throwable> first,
        @NotNull Class<? extends Throwable> second, @NotNull Block<T> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("second", second),
          new IfNull("block", block)
      ).throwException();
      this.types = Arrays.asList(first, second);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends Throwable> first,
        @NotNull Class<? extends Throwable> second, @NotNull Class<? extends Throwable> third,
        @NotNull Block<T> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("second", second),
          new IfNull("third", third),
          new IfNull("block", block)
      ).throwException();
      this.types = Arrays.asList(first, second, third);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends Throwable> first,
        @NotNull Class<? extends Throwable> second, @NotNull Class<? extends Throwable> third,
        @NotNull Class<? extends Throwable> fourth, @NotNull Block<T> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("second", second),
          new IfNull("third", third),
          new IfNull("fourth", fourth),
          new IfNull("block", block)
      ).throwException();
      this.types = Arrays.asList(first, second, third, fourth);
      this.block = block;
    }

    public Catch(@NotNull List<Class<? extends Throwable>> types, @NotNull Block<T> block) {
      new IfSomeOf(
          new IfAnyOf(
              new IfNull("types", types),
              new IfContainsNull("types", types)
          ),
          new IfNull("block", block)
      ).throwException();
      this.types = types;
      this.block = block;
    }

    @NotNull
    public Result<T> execute(@NotNull Throwable throwable) throws Exception {
      for (Class<? extends Throwable> type : types) {
        if (type.isInstance(throwable)) {
          return block.execute(throwable);
        }
      }
      return new Continue<T>();
    }
  }

  private static class TryAwaitable<T, M> extends BaseAwaitable<T> implements Awaiter<M> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final Scheduler scheduler;
    private final int maxEvents;
    private final Awaitable<M> awaitable;
    private final Block<T, ? super M> block;
    private Cancelable cancelable;
    private State<T> state = input;
    private int events;

    private TryAwaitable(@NotNull Scheduler scheduler, int maxEvents,
        @NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
      super(scheduler);
      this.scheduler = scheduler;
      this.maxEvents = maxEvents;
      this.awaitable = awaitable;
      this.block = block;
    }

    @Override
    protected boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    @Override
    protected void cancelExecution() {
      state.cancelExecution();
    }

    public void message(M message) {
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
          inputs.offer(STOP);
          state = new EndState();
          scheduleFlow();
        }
      });
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;

      void cancelExecution();
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        state = message;
        events = maxEvents;
        cancelable = awaitable.await(events, TryAwaitable.this);
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }

      public void cancelExecution() {
        final Cancelable cancelable = TryAwaitable.this.cancelable;
        if (cancelable != null) {
          cancelable.cancel();
        }
      }
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        final Object message = inputs.poll();
        if (message != null) {
          if (maxEvents >= 0) {
            --events;
          }
          execute(flowControl, message);
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          cancelable = awaitable.await(events, TryAwaitable.this);
        }
        return false;
      }

      public void cancelExecution() {
        final Cancelable cancelable = TryAwaitable.this.cancelable;
        if (cancelable != null) {
          cancelable.cancel();
        }
      }

      @SuppressWarnings("unchecked")
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        flowControl.logger().log(
            new DbgMessage("[executing] block: %s", new PrintIdentity(block))
        );
        block.execute(message != NULL ? (M) message : null).apply(flowControl);
      }
    }

    private class ErrorState extends MessageState {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      @Override
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        if (message == STOP) {
          flowControl.abort(error);
        } else {
          super.execute(flowControl, message);
        }
      }

      @Override
      public void cancelExecution() {
      }
    }

    private class EndState extends MessageState {

      @Override
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        if (message == STOP) {
          flowControl.stop();
        } else {
          super.execute(flowControl, message);
        }
      }

      @Override
      public void cancelExecution() {
      }
    }
  }
}
