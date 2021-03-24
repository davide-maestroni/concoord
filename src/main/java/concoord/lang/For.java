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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.flow.Result;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
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

  public For(int maxEvents, @NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
    new IfSomeOf(
        new IfNull("awaitable", awaitable),
        new IfNull("block", block)
    ).throwException();
    this.maxEvents = maxEvents;
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(
        scheduler,
        new ForControl<T, M>(scheduler, maxEvents, awaitable, block)
    );
  }

  public interface Block<T, M> {

    @NotNull
    Result<T> execute(M message) throws Exception;
  }

  private static class ForControl<T, M> implements ExecutionControl<T> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final Scheduler scheduler;
    private final int maxEvents;
    private final Awaitable<M> awaitable;
    private final Block<T, ? super M> block;
    private State<T> state = input;
    private int events;

    private ForControl(@NotNull Scheduler scheduler, int maxEvents,
        @NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
      this.scheduler = scheduler;
      this.maxEvents = maxEvents;
      this.awaitable = awaitable;
      this.block = block;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    public void abortExecution(@NotNull Throwable error) {
      awaitable.abort();
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class ForAwaiter implements Awaiter<M> {

      private final FlowCommand flow = new FlowCommand();
      private final AwaitableFlowControl<T> flowControl;

      private ForAwaiter(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void message(M message) {
        inputs.offer(message != null ? message : NULL);
        scheduler.scheduleLow(flow);
      }

      public void error(@NotNull Throwable error) {
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
          state = new ErrorState(error);
          flowControl.execute();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          inputs.offer(STOP);
          state = new EndState();
          flowControl.execute();
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        state = message;
        events = maxEvents;
        awaitable.await(events, new ForAwaiter(flowControl));
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }
    }

    private class MessageState implements State<T> {

      @SuppressWarnings("unchecked")
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        final Object message = inputs.poll();
        if (message != null) {
          if (maxEvents >= 0) {
            --events;
          }
          flowControl.logger().log(
              new DbgMessage("[executing] block: %s", new PrintIdentity(block))
          );
          block.execute(message != NULL ? (M) message : null).apply(flowControl);
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          awaitable.await(events, new ForAwaiter(flowControl));
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
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (inputs.peek() == STOP) {
          flowControl.error(error);
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (inputs.peek() == STOP) {
          flowControl.stop();
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }
  }
}
