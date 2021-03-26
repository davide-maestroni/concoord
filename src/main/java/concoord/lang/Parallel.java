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
import concoord.data.Buffer;
import concoord.data.BufferFactory;
import concoord.data.DefaultBufferFactory;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.lang.For.Block;
import concoord.logging.DbgMessage;
import concoord.logging.PrintIdentity;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Parallel<T, M> implements Task<T> {

  private final int maxEvents;
  private final BufferFactory<T> factory;
  private final SchedulingStrategy<? super M> strategy;
  private final Awaitable<M> awaitable;
  private final Block<? extends T, ? super M> block;

  public Parallel(@NotNull SchedulingStrategy<? super M> strategy,
      @NotNull Awaitable<M> awaitable, @NotNull Block<? extends T, ? super M> block) {
    this(1, new DefaultBufferFactory<T>(), strategy, awaitable, block);
  }

  public Parallel(int maxEvents, @NotNull SchedulingStrategy<? super M> strategy,
      @NotNull Awaitable<M> awaitable, @NotNull Block<T, ? super M> block) {
    this(maxEvents, new DefaultBufferFactory<T>(), strategy, awaitable, block);
  }

  public Parallel(int maxEvents, int initialCapacity,
      @NotNull SchedulingStrategy<? super M> strategy, @NotNull Awaitable<M> awaitable,
      @NotNull Block<T, ? super M> block) {
    this(maxEvents, new DefaultBufferFactory<T>(initialCapacity), strategy, awaitable, block);
  }

  public Parallel(@NotNull BufferFactory<T> factory, @NotNull SchedulingStrategy<? super M> strategy,
      @NotNull Awaitable<M> awaitable, @NotNull Block<? extends T, ? super M> block) {
    this(1, factory, strategy, awaitable, block);
  }

  public Parallel(int maxEvents, @NotNull BufferFactory<T> factory,
      @NotNull SchedulingStrategy<? super M> strategy, @NotNull Awaitable<M> awaitable,
      @NotNull Block<? extends T, ? super M> block) {
    new IfSomeOf(
        new IfNull("factory", factory),
        new IfNull("strategy", strategy),
        new IfNull("awaitable", awaitable),
        new IfNull("block", block)
    ).throwException();
    this.maxEvents = maxEvents;
    this.factory = factory;
    this.strategy = strategy;
    this.awaitable = awaitable;
    this.block = block;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(
        scheduler,
        new ParallelControl<T, M>(
            scheduler,
            maxEvents,
            factory,
            strategy,
            awaitable,
            block
        )
    );
  }

  public interface SchedulingStrategy<M> {

    @NotNull
    Scheduler nextScheduler(M message) throws Exception;
  }

  private static class ParallelControl<T, M> implements ExecutionControl<T> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
    private final ArrayList<Awaitable<T>> awaitables = new ArrayList<Awaitable<T>>();
    private final MessageState message = new MessageState();
    private final Scheduler scheduler;
    private final int maxEvents;
    private final BufferFactory<T> factory;
    private final SchedulingStrategy<? super M> strategy;
    private final Awaitable<M> awaitable;
    private final Block<? extends T, ? super M> block;
    private State<T> state = new InputState();
    private Buffer<T> buffer;
    private Iterator<T> inputs;
    private int events;

    private ParallelControl(@NotNull Scheduler scheduler, int maxEvents,
        @NotNull BufferFactory<T> factory, @NotNull SchedulingStrategy<? super M> strategy,
        @NotNull Awaitable<M> awaitable, @NotNull Block<? extends T, ? super M> block) {
      this.scheduler = scheduler;
      this.maxEvents = maxEvents;
      this.factory = factory;
      this.strategy = strategy;
      this.awaitable = awaitable;
      this.block = block;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    public void abortExecution(@NotNull Throwable error) {
      this.awaitable.abort();
      for (final Awaitable<T> awaitable : awaitables) {
        awaitable.abort();
      }
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class ParallelAwaiter implements Awaiter<M> {

      private final FlowCommand flow = new FlowCommand();
      private final AwaitableFlowControl<T> flowControl;

      private ParallelAwaiter(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void message(M message) {
        scheduler.scheduleLow(new MessageCommand(message));
      }

      public void error(@NotNull Throwable error) {
        scheduler.scheduleLow(new ErrorCommand(error));
      }

      public void end() {
        scheduler.scheduleLow(new EndCommand());
      }

      private class FlowCommand implements Runnable {

        @SuppressWarnings("unchecked")
        public void run() {
          final ConcurrentLinkedQueue<Object> queue = ParallelControl.this.queue;
          final Object message = queue.peek();
          if ((message != null) && (message != STOP)) {
            queue.remove();
            buffer.add(message != NULL ? (T) message : null);
          }
          flowControl.execute();
        }
      }

      private class MessageCommand implements Runnable {

        private final M message;

        private MessageCommand(M message) {
          this.message = message;
        }

        @SuppressWarnings("unchecked")
        public void run() {
          final M message = this.message;
          try {
            final Scheduler scheduler = strategy.nextScheduler(message);
            flowControl.logger().log(
                new DbgMessage(
                    "[scheduling] block: %s, on scheduler: %s",
                    new PrintIdentity(block),
                    new PrintIdentity(scheduler)
                )
            );
            final Awaitable<T> awaitable = new For<T, M>(
                new Iter<M>(Collections.singleton(message)).on(scheduler),
                (Block<T, ? super M>) block
            )
                .on(scheduler);
            awaitables.add(awaitable);
            awaitable.await(-1, new ForAwaiter(awaitable));
          } catch (final Exception e) {
            queue.offer(STOP);
            state = new ErrorState(e);
            flowControl.execute();
          }
        }
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          queue.offer(STOP);
          state = new ErrorState(error);
          flowControl.execute();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          queue.offer(STOP);
          state = new EndState();
          flowControl.execute();
        }
      }

      private class RemoveCommand implements Runnable {

        private final Awaitable<T> awaitable;

        private RemoveCommand(@NotNull Awaitable<T> awaitable) {
          this.awaitable = awaitable;
        }

        public void run() {
          awaitables.remove(awaitable);
        }
      }

      private class ForAwaiter implements Awaiter<T> {

        private final Awaitable<T> awaitable;

        private ForAwaiter(@NotNull Awaitable<T> awaitable) {
          this.awaitable = awaitable;
        }

        public void message(T message) throws Exception {
          queue.offer(message != null ? message : NULL);
          scheduler.scheduleLow(flow);
        }

        public void error(@NotNull Throwable error) throws Exception {
          scheduler.scheduleLow(new ErrorCommand(error));
        }

        public void end() throws Exception {
          scheduler.scheduleLow(new RemoveCommand(awaitable));
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        buffer = factory.create();
        inputs = buffer.iterator();
        state = message;
        events = maxEvents;
        awaitable.await(events, new ParallelAwaiter(flowControl));
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
      }
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (inputs.hasNext()) {
          if (maxEvents >= 0) {
            --events;
          }
          flowControl.postOutput(inputs.next());
          return true;
        }
        if (events < 1) {
          events = flowControl.inputEvents();
          awaitable.await(events, new ParallelAwaiter(flowControl));
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
        if (!inputs.hasNext() && (queue.peek() == STOP)) {
          flowControl.error(error);
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (!inputs.hasNext() && (queue.peek() == STOP)) {
          flowControl.stop();
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }
  }
}
