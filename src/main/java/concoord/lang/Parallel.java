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
import concoord.lang.BaseAwaitable.BaseFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import concoord.util.logging.ErrMessage;
import concoord.util.logging.LogMessage;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Parallel<T, M> implements Task<T> {

  private final Awaitable<M> awaitable;
  private final BufferControl<T> bufferControl;
  private final SchedulingStrategyFactory<? extends T, ? super M> strategyFactory;

  Parallel(@NotNull Awaitable<M> awaitable, @NotNull BufferControl<T> bufferControl,
      @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
    new IfSomeOf(
        new IfNull("strategyFactory", strategyFactory),
        new IfNull("bufferControl", bufferControl),
        new IfNull("awaitable", awaitable)
    ).throwException();
    this.awaitable = awaitable;
    this.bufferControl = bufferControl;
    this.strategyFactory = strategyFactory;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(
        scheduler,
        new ParallelControl<T, M>(scheduler, awaitable, bufferControl, strategyFactory)
    );
  }

  interface SchedulingControl<T, M> {

    void schedule(M message) throws Exception;

    void abort();

    void pause();

    void resume();

    void stop();

    boolean isSettled();

    boolean hasNext();

    T next();
  }

  interface BufferControl<M> {

    @NotNull
    InputChannel<M> inputChannel() throws Exception;

    @NotNull
    OutputChannel<M> outputChannel() throws Exception;
  }

  interface InputChannel<M> {

    void push(M message);

    void close();
  }

  interface OutputChannel<M> {

    boolean hasNext();

    M next();
  }

  public interface Block<T, M> {

    @NotNull
    Awaitable<T> execute(@NotNull Scheduler scheduler, @NotNull Awaitable<M> awaitable)
        throws Exception;
  }

  public interface SchedulingStrategy<T, M> {

    @NotNull
    Awaitable<T> schedule(M message) throws Exception;
  }

  public interface SchedulingStrategyFactory<T, M> {

    @NotNull
    SchedulingStrategy<T, M> create() throws Exception;
  }

  private static class ParallelControl<T, M> implements ExecutionControl<T> {

    private static final Object NULL = new Object();

    private final IdentityHashMap<Awaitable<? extends T>, Restartable> awaitables =
        new IdentityHashMap<Awaitable<? extends T>, Restartable>();
    private final Scheduler scheduler;
    private final Awaitable<M> awaitable;
    private final BufferControl<T> bufferControl;
    private final SchedulingStrategyFactory<? extends T, ? super M> strategyFactory;
    private State<T> controlState = new InputState();
    private SchedulingStrategy<? extends T, ? super M> strategy;
    private OutputChannel<T> outputChannel;
    private ParallelAwaiter awaiter;
    private Cancelable cancelable;

    private ParallelControl(@NotNull Scheduler scheduler, @NotNull Awaitable<M> awaitable,
        @NotNull BufferControl<T> bufferControl,
        @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
      this.scheduler = scheduler;
      this.awaitable = awaitable;
      this.bufferControl = bufferControl;
      this.strategyFactory = strategyFactory;
    }

    public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      final Cancelable cancelable = this.cancelable;
      if (cancelable != null) {
        cancelable.cancel();
      }
      for (final Restartable restartable : awaitables.values()) {
        restartable.cancel();
      }
    }

    public void abortExecution(@NotNull Throwable error) {
      this.awaitable.abort();
      for (final Awaitable<? extends T> awaitable : awaitables.keySet()) {
        awaitable.abort();
      }
    }

    private interface Restartable {

      void start();

      void cancel();
    }

    private interface State<T> {

      boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception;
    }

    private class ParallelAwaiter implements Awaiter<M> {

      private final ConcurrentLinkedQueue<Object> inputQueue = new ConcurrentLinkedQueue<Object>();
      private final InputMessageCommand inputMessageCommand = new InputMessageCommand();
      private State<T> awaiterState = new ReadState();
      private BaseFlowControl<T> flowControl;
      private int eventCount;

      public void message(M message) {
        inputQueue.offer(message != null ? message : NULL);
        scheduler.scheduleLow(inputMessageCommand);
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

      private boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
        this.flowControl = flowControl;
        return awaiterState.executeBlock(flowControl);
      }

      private class InputMessageCommand implements Runnable {

        @SuppressWarnings("unchecked")
        public void run() {
          final Object message = inputQueue.poll();
          if (message == null) {
            return;
          }
          if (eventCount > 0) {
            --eventCount;
          }
          final BaseFlowControl<T> flowControl = ParallelAwaiter.this.flowControl;
          final M input = message != NULL ? (M) message : null;
          try {
//            final Scheduler scheduler = strategy.schedule(input);
//            flowControl.logger().log(
//                new DbgMessage(
//                    "[scheduling] block: %s, on scheduler: %s",
//                    new PrintIdentity(block),
//                    new PrintIdentity(scheduler)
//                )
//            );
            final Awaitable<? extends T> awaitable = strategy.schedule(input);
            if (!awaitables.containsKey(awaitable)) {
              // TODO: 03/04/21 log
              new ForAwaiter(awaitable, bufferControl.inputChannel()).start();
            }
          } catch (final Exception e) {
            flowControl.logger().log(
                new ErrMessage(new LogMessage("failed to schedule next block"), e)
            );
            awaiterState = new ErrorState(e);
            flowControl.execute();
          }
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

      private class ReadState implements State<T> {

        public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) {
          if (eventCount == 0) {
            eventCount = flowControl.outputEvents();
            cancelable = awaitable.await(eventCount, ParallelAwaiter.this);
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

        public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) {
          flowControl.error(error);
          cancelExecution(); // TODO: 18/03/21 ???
          return true;
        }
      }

      private class EndState implements State<T> {

        public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) {
          if (awaitables.isEmpty()) {
            flowControl.stop();
            return true;
          }
          return false;
        }
      }

      private class ForAwaiter implements Awaiter<T>, Restartable {

        private final OutputMessageCommand outputMessageCommand = new OutputMessageCommand();
        private final ConcurrentLinkedQueue<Object> outputQueue = new ConcurrentLinkedQueue<Object>();
        private final Awaitable<? extends T> awaitable;
        private final InputChannel<T> channel;
        private Cancelable cancelable;

        private ForAwaiter(@NotNull Awaitable<? extends T> awaitable,
            @NotNull InputChannel<T> channel) {
          this.awaitable = awaitable;
          this.channel = channel;
        }

        public void message(T message) {
          outputQueue.offer(message != null ? message : NULL);
          scheduler.scheduleLow(outputMessageCommand);
        }

        public void error(@NotNull Throwable error) {
          if (error instanceof CancelException) {
            scheduler.scheduleLow(new CancelCommand());
          } else {
            scheduler.scheduleLow(new ErrorCommand(error));
          }
        }

        public void end() {
          scheduler.scheduleLow(new EndCommand(awaitable));
        }

        public void start() {
          if (cancelable == null) {
            cancelable = awaitable.await(-1, this);
            awaitables.put(awaitable, this);
          }
        }

        public void cancel() {
          if (cancelable != null) {
            cancelable.cancel();
          }
        }

        private class OutputMessageCommand implements Runnable {

          @SuppressWarnings("unchecked")
          public void run() {
            final Object message = outputQueue.poll();
            if (message != null) {
              channel.push(message != NULL ? (T) message : null);
              flowControl.execute();
            }
          }
        }

        private class CancelCommand implements Runnable {

          public void run() {
            cancelable = null;
          }
        }

        private class EndCommand implements Runnable {

          private final Awaitable<? extends T> awaitable;

          private EndCommand(@NotNull Awaitable<? extends T> awaitable) {
            this.awaitable = awaitable;
          }

          public void run() {
            channel.close();
            awaitables.remove(awaitable);
            flowControl.execute();
          }
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
        strategy = strategyFactory.create();
        outputChannel = bufferControl.outputChannel();
        controlState = new MessageState();
        awaiter = new ParallelAwaiter();
        awaiter.executeBlock(flowControl);
        return false;
      }
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
        final OutputChannel<T> outputChannel = ParallelControl.this.outputChannel;
        if (outputChannel.hasNext()) {
          flowControl.postOutput(outputChannel.next());
          return true;
        }
        final IdentityHashMap<Awaitable<? extends T>, Restartable> awaitables =
            ParallelControl.this.awaitables;
        if (!awaitables.isEmpty()) {
          for (final Restartable restartable : awaitables.values()) {
            restartable.start();
          }
          return false;
        }
        return awaiter.executeBlock(flowControl);
      }
    }
  }
}
