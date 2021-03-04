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
import concoord.flow.Result;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class For<T, M> implements Task<T> {

  private final int maxEvents;
  private final Awaitable<M> awaitable;
  private final Block<T, ? super M> block;

  public For(@NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
    this(1, awaitable, block);
  }

  public For(int maxEvents, @NotNull Awaitable<M> awaitable,
      @NotNull Block<T, ? super M> block) {
    new IfSomeOf(
        new IfNull(awaitable, "awaitable"),
        new IfNull(block, "block")
    ).throwException();
    this.maxEvents = maxEvents;
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    new IfNull(scheduler, "scheduler").throwException();
    return new ForAwaitable<T, M>(scheduler, maxEvents, awaitable, block);
  }

  public interface Block<T, M> {

    @NotNull
    Result<T> execute(M message) throws Exception;
  }

  private static class ForAwaitable<T, M> extends BaseAwaitable<T> implements Awaiter<M> {

    private static final Object NULL = new Object();

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

    private ForAwaitable(@NotNull Scheduler scheduler, int maxEvents,
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
          state = new ErrorState(error);
          scheduleFlow();
        }
      });
    }

    public void end() {
      scheduler.scheduleLow(new Runnable() {
        public void run() {
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
        cancelable = awaitable.await(events, ForAwaitable.this);
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }

      public void cancelExecution() {
        final Cancelable cancelable = ForAwaitable.this.cancelable;
        if (cancelable != null) {
          cancelable.cancel();
        }
      }
    }

    private class MessageState implements State<T> {

      @SuppressWarnings("unchecked")
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        final Object message = inputs.poll();
        if (message != null) {
          flowControl.logger().log(
              new DbgMessage("[executing] block: %s", new PrintIdentity(block))
          );
          if (maxEvents >= 0) {
            --events;
          }
          block.execute(message != NULL ? (M) message : null).apply(flowControl);
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          cancelable = awaitable.await(events, ForAwaitable.this);
        }
        return false;
      }

      public void cancelExecution() {
        final Cancelable cancelable = ForAwaitable.this.cancelable;
        if (cancelable != null) {
          cancelable.cancel();
        }
      }
    }

    private class ErrorState extends MessageState {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (!super.executeBlock(flowControl)) {
          flowControl.abort(error);
          return false;
        }
        return true;
      }

      @Override
      public void cancelExecution() {
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (!super.executeBlock(flowControl)) {
          flowControl.stop();
        }
        return true;
      }

      @Override
      public void cancelExecution() {
      }
    }
  }
}
