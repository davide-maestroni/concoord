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
import concoord.lang.For.Block;
import concoord.logging.DbgMessage;
import concoord.logging.ErrMessage;
import concoord.logging.LogMessage;
import concoord.logging.PrintIdentity;
import concoord.scheduling.SchedulingStrategy;
import concoord.scheduling.SchedulingStrategyFactory;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

class Parallel<T, M> implements Task<T> {

  private final SchedulingStrategyFactory<? super M> strategyFactory;
  private final BufferControl<T> bufferControl;
  private final Awaitable<M> awaitable;
  private final Block<? extends T, ? super M> block;

  Parallel(@NotNull SchedulingStrategyFactory<? super M> strategyFactory,
      @NotNull BufferControl<T> bufferControl, @NotNull Awaitable<M> awaitable,
      @NotNull Block<? extends T, ? super M> block) {
    new IfSomeOf(
        new IfNull("strategyFactory", strategyFactory),
        new IfNull("bufferControl", bufferControl),
        new IfNull("awaitable", awaitable),
        new IfNull("block", block)
    ).throwException();
    this.bufferControl = bufferControl;
    this.strategyFactory = strategyFactory;
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(
        scheduler,
        new ParallelControl<T, M>(scheduler, strategyFactory, bufferControl, awaitable, block)
    );
  }

  interface BufferControl<M> {

    @NotNull
    InputChannel<M> inputChannel();

    @NotNull
    OutputChannel<M> outputChannel();
  }

  interface InputChannel<M> {

    void push(M message);

    void close();
  }

  interface OutputChannel<M> {

    boolean hasNext();

    M next();
  }

  private static class ParallelControl<T, M> implements ExecutionControl<T> {

    private static final Object NULL = new Object();

    private final IdentityHashMap<Awaitable<T>, Restartable> awaitables =
        new IdentityHashMap<Awaitable<T>, Restartable>();
    private final MessageState messageState = new MessageState();
    private final Scheduler scheduler;
    private final SchedulingStrategyFactory<? super M> strategyFactory;
    private final BufferControl<T> bufferControl;
    private final Awaitable<M> awaitable;
    private final Block<? extends T, ? super M> block;
    private State<T> currentState = new InputState();
    private SchedulingStrategy<? super M> strategy;
    private OutputChannel<T> outputChannel;
    private ParallelAwaiter awaiter;
    private Cancelable cancelable;

    private ParallelControl(@NotNull Scheduler scheduler,
        @NotNull SchedulingStrategyFactory<? super M> strategyFactory,
        @NotNull BufferControl<T> bufferControl, @NotNull Awaitable<M> awaitable,
        @NotNull Block<? extends T, ? super M> block) {
      this.scheduler = scheduler;
      this.strategyFactory = strategyFactory;
      this.bufferControl = bufferControl;
      this.awaitable = awaitable;
      this.block = block;
    }

    public boolean executeBlock(@NotNull BaseFlowControl<T> flowControl) throws Exception {
      return currentState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      final Cancelable cancelable = this.cancelable;
      if (cancelable != null) {
        cancelable.cancel();
      }
    }

    public void abortExecution(@NotNull Throwable error) {
      this.awaitable.abort();
      for (final Awaitable<T> awaitable : awaitables.keySet()) {
        awaitable.abort();
      }
      for (final Restartable restartable : awaitables.values()) {
        restartable.cancel();
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
            final Scheduler scheduler = strategy.nextScheduler(input);
            flowControl.logger().log(
                new DbgMessage(
                    "[scheduling] block: %s, on scheduler: %s",
                    new PrintIdentity(block),
                    new PrintIdentity(scheduler)
                )
            );
            final Awaitable<T> awaitable = new For<T, M>(
                new Iter<M>(input).on(scheduler),
                (Block<T, ? super M>) block
            ).on(scheduler);
            new ForAwaiter(awaitable, bufferControl.inputChannel()).start();
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
        private final Awaitable<T> awaitable;
        private final InputChannel<T> channel;
        private Cancelable cancelable;

        private ForAwaiter(@NotNull Awaitable<T> awaitable, @NotNull InputChannel<T> channel) {
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

          private final Awaitable<T> awaitable;

          private EndCommand(@NotNull Awaitable<T> awaitable) {
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
        currentState = messageState;
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
        final IdentityHashMap<Awaitable<T>, Restartable> awaitables =
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
