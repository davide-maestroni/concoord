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
import concoord.flow.Result;
import concoord.lang.StandardAwaitable.ExecutionControl;
import concoord.lang.StandardAwaitable.StandardFlowControl;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import concoord.util.logging.DbgMessage;
import concoord.util.logging.PrintIdentity;
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
    return new StandardAwaitable<T>(
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

    private final ConcurrentLinkedQueue<Object> inputQueue = new ConcurrentLinkedQueue<Object>();
    private final Scheduler scheduler;
    private final int maxEvents;
    private final Awaitable<M> awaitable;
    private final Block<T, ? super M> block;
    private State<T> controlState = new InputState();
    private ForAwaiter awaiter;
    private Cancelable cancelable;

    private ForControl(@NotNull Scheduler scheduler, int maxEvents,
        @NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
      this.scheduler = scheduler;
      this.maxEvents = maxEvents;
      this.awaitable = awaitable;
      this.block = block;
    }

    public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      final Cancelable cancelable = this.cancelable;
      if (cancelable != null) {
        cancelable.cancel();
      }
    }

    public void abortExecution(@NotNull Throwable error) {
      awaitable.abort();
    }

    private interface State<T> {

      boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception;
    }

    private class ForAwaiter implements Awaiter<M> {

      private final MessageCommand messageCommand = new MessageCommand();
      private State<T> awaiterState = new InitState();
      private StandardFlowControl<T> flowControl;
      private int eventCount;

      public void message(M message) {
        inputQueue.offer(message != null ? message : NULL);
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

      private boolean executeBlock(@NotNull StandardFlowControl<T> flowControl)
          throws Exception {
        this.flowControl = flowControl;
        return awaiterState.executeBlock(flowControl);
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

      private class InitState implements State<T> {

        public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) {
          awaiterState = new ReadState();
          eventCount = maxEvents;
          cancelable = awaitable.await(eventCount, ForAwaiter.this);
          return false;
        }
      }

      private class ReadState implements State<T> {

        public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) {
          if (eventCount == 0) {
            eventCount = flowControl.inputEvents();
            cancelable = awaitable.await(eventCount, ForAwaiter.this);
            return false;
          }
          return true;
        }
      }

      private class ErrorState implements State<T> {

        private final Throwable error;

        private ErrorState(@NotNull Throwable error) {
          this.error = error;
        }

        public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) {
          flowControl.error(error);
          return true;
        }
      }

      private class EndState implements State<T> {

        public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) {
          flowControl.stop();
          return true;
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        controlState = new MessageState();
        awaiter = new ForAwaiter();
        awaiter.executeBlock(flowControl);
        return false;
      }
    }

    private class MessageState implements State<T> {

      @SuppressWarnings("unchecked")
      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        final Object message = inputQueue.poll();
        if (message != null) {
          flowControl.logger().log(
              new DbgMessage("[executing] block: %s", new PrintIdentity(block))
          );
          block.execute(message != NULL ? (M) message : null).apply(flowControl);
          return true;
        }
        return awaiter.executeBlock(flowControl);
      }
    }
  }
}
