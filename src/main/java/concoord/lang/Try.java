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
import concoord.flow.Break;
import concoord.flow.Continue;
import concoord.flow.FlowControl;
import concoord.flow.Result;
import concoord.flow.Yield;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.logging.DbgMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.PrintIdentity;
import concoord.logging.WrnMessage;
import concoord.util.assertion.IfAnyOf;
import concoord.util.assertion.IfContainsNull;
import concoord.util.assertion.IfInterrupt;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Try<T> implements Task<T> {

  private final Awaitable<T> awaitable;
  private final List<Block<? extends T, ? super Throwable>> blocks;

  public Try(@NotNull Awaitable<T> awaitable,
      @NotNull Block<? extends T, ? super Throwable>... blocks) {
    new IfSomeOf(
        new IfNull("awaitable", awaitable),
        new IfAnyOf(
            new IfNull("blocks", blocks),
            new IfContainsNull("blocks", (Object[]) blocks)
        )
    ).throwException();
    this.awaitable = awaitable;
    this.blocks = Arrays.asList(blocks);
  }

  public Try(@NotNull Awaitable<T> awaitable,
      @NotNull List<Block<? extends T, ? super Throwable>> blocks) {
    new IfSomeOf(
        new IfNull("awaitable", awaitable),
        new IfAnyOf(
            new IfNull("blocks", blocks),
            new IfContainsNull("blocks", blocks)
        )
    ).throwException();
    this.awaitable = awaitable;
    this.blocks = blocks;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseAwaitable<T>(scheduler, new TryControl<T>(scheduler, awaitable, blocks));
  }

  public interface Block<T, E extends Throwable> {

    @NotNull
    Result<? extends T> execute(@NotNull E error) throws Exception;
  }

  public static class Catch<T, E extends Throwable> implements Block<T, Throwable> {

    private final List<? extends Class<? extends E>> types;
    private final Block<? extends T, ? super E> block;

    public Catch(@NotNull Class<? extends E> first, @NotNull Block<? extends T, ? super E> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("block", block)
      ).throwException();
      this.types = Collections.<Class<? extends E>>singletonList(first);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends E> first,
        @NotNull Class<? extends E> second, @NotNull Block<? extends T, ? super E> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("second", second),
          new IfNull("block", block)
      ).throwException();
      this.types = Arrays.asList(first, second);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends E> first,
        @NotNull Class<? extends E> second, @NotNull Class<? extends E> third,
        @NotNull Block<? extends T, ? super E> block) {
      new IfSomeOf(
          new IfNull("first", first),
          new IfNull("second", second),
          new IfNull("third", third),
          new IfNull("block", block)
      ).throwException();
      this.types = Arrays.asList(first, second, third);
      this.block = block;
    }

    public Catch(@NotNull Class<? extends E> first,
        @NotNull Class<? extends E> second, @NotNull Class<? extends E> third,
        @NotNull Class<? extends E> fourth, @NotNull Block<? extends T, ? super E> block) {
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

    public Catch(@NotNull List<? extends Class<? extends E>> types,
        @NotNull Block<? extends T, ? super E> block) {
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
    @SuppressWarnings("unchecked")
    public final Result<? extends T> execute(@NotNull Throwable error) throws Exception {
      for (final Class<? extends E> type : types) {
        if (type.isInstance(error)) {
          return block.execute((E) error);
        }
      }
      return new Continue<T>();
    }
  }

  public static class Finally<T> implements Block<T, Throwable> {

    private final Block<? extends T, ? super Throwable> block;

    public Finally(@NotNull final VoidBlock block) {
      new IfNull("block", block).throwException();
      this.block = new Block<T, Throwable>() {
        @NotNull
        public Result<? extends T> execute(@NotNull Throwable error) throws Exception {
          block.execute();
          return new Continue<T>();
        }
      };
    }

    public Finally(@NotNull final ResultBlock<T> block) {
      new IfNull("block", block).throwException();
      this.block = new Block<T, Throwable>() {
        @NotNull
        public Result<? extends T> execute(@NotNull Throwable error) throws Exception {
          return block.execute();
        }
      };
    }

    public Finally(@NotNull final Block<? extends T, ? super Throwable> block) {
      new IfNull("block", block).throwException();
      this.block = block;
    }

    @NotNull
    public Result<? extends T> execute(@NotNull Throwable error) throws Exception {
      return block.execute(error);
    }

    public interface VoidBlock {

      void execute() throws Exception;
    }

    public interface ResultBlock<T> {

      @NotNull
      Result<? extends T> execute() throws Exception;
    }
  }

  private static class TryControl<T> implements ExecutionControl<T> {

    private static final Object NULL = new Object();
    private static final Object STOP = new Object();

    private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
    private final InputState input = new InputState();
    private final MessageState message = new MessageState();
    private final Scheduler scheduler;
    private final Awaitable<T> awaitable;
    private final List<Block<? extends T, ? super Throwable>> blocks;
    private State<T> state = input;
    private int maxEvents;
    private int events;

    private TryControl(@NotNull Scheduler scheduler, @NotNull Awaitable<T> awaitable,
        @NotNull List<Block<? extends T, ? super Throwable>> blocks) {
      this.scheduler = scheduler;
      this.awaitable = awaitable;
      this.blocks = blocks;
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return state.executeBlock(flowControl);
    }

    public void abortExecution() {
      awaitable.abort();
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private static class ErrorFlowControl<T> implements FlowControl<T> {

      private final Logger logger;
      private Result<T> result;
      private boolean stopped;

      public ErrorFlowControl(@NotNull Logger logger) {
        this.logger = logger;
      }

      public void postOutput(T message) {
        if (result != null) {
          logger.log(new WrnMessage("original output overwritten by: %s", message));
        }
        result = new Yield<T>(message);
      }

      public void postOutput(Awaitable<? extends T> awaitable) {
        if (result != null) {
          logger.log(
              new WrnMessage("original output overwritten by: %s", new PrintIdentity(awaitable))
          );
        }
        result = new Yield<T>(awaitable);
      }

      public void nextInputs(int maxEvents) {
        // ignore
      }

      public void stop() {
        if (result == null) {
          result = new Break<T>();
        }
        stopped = true;
      }

      private boolean isStopped() {
        return stopped;
      }

      private void applyResult(@NotNull AwaitableFlowControl<? super T> flowControl,
          @NotNull Throwable error) {
        if (result != null) {
          result.apply(flowControl);
          flowControl.stop();
        } else {
          flowControl.error(error);
        }
      }
    }

    private static class EndFlowControl<T> implements FlowControl<T> {

      private final Logger logger;
      private Result<T> result;

      public EndFlowControl(@NotNull Logger logger) {
        this.logger = logger;
      }

      public void postOutput(T message) {
        logger.log(new WrnMessage("original output overwritten by: %s", message));
        result = new Yield<T>(message);
      }

      public void postOutput(Awaitable<? extends T> awaitable) {
        logger.log(
            new WrnMessage("original output overwritten by: %s", new PrintIdentity(awaitable))
        );
        result = new Yield<T>(awaitable);
      }

      public void nextInputs(int maxEvents) {
        // ignore
      }

      public void stop() {
        if (result == null) {
          result = new Break<T>();
        }
      }

      private void applyResult(@NotNull AwaitableFlowControl<? super T> flowControl,
          @NotNull Throwable error) {
        if (result != null) {
          result.apply(flowControl);
          flowControl.stop();
        } else {
          flowControl.error(error);
        }
      }
    }

    private class TryAwaiter implements Awaiter<T> {

      private final AwaitableFlowControl<T> flowControl;

      private TryAwaiter(@NotNull AwaitableFlowControl<T> flowControl) {
        this.flowControl = flowControl;
      }

      public void message(T message) {
        inputs.offer(message != null ? message : NULL);
        flowControl.schedule();
      }

      public void error(@NotNull Throwable error) {
        scheduler.scheduleLow(new ErrorCommand(error));
      }

      public void end(int reason) {
        final Runnable command;
        if (reason == Awaiter.DONE) {
          command = new EndCommand();
        } else {
          command = new AbortCommand();
        }
        scheduler.scheduleLow(command);
      }

      private class ErrorCommand implements Runnable {

        private final Throwable error;

        private ErrorCommand(@NotNull Throwable error) {
          this.error = error;
        }

        public void run() {
          inputs.offer(STOP);
          state = new ErrorState(error);
          flowControl.schedule();
        }
      }

      private class EndCommand implements Runnable {

        public void run() {
          inputs.offer(STOP);
          state = new EndState();
          flowControl.schedule();
        }
      }

      private class AbortCommand implements Runnable {

        public void run() {
          inputs.offer(STOP);
          state = new AbortState();
          flowControl.schedule();
        }
      }
    }

    private class InputState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) {
        state = message;
        events = maxEvents = flowControl.outputEvents();
        awaitable.await(events, new TryAwaiter(flowControl));
        if (maxEvents < 0) {
          events = 1;
        }
        return false;
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
          awaitable.await(events, new TryAwaiter(flowControl));
        }
        return false;
      }

      @SuppressWarnings("unchecked")
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        flowControl.postOutput(message != NULL ? (T) message : null);
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
          if (blocks.isEmpty()) {
            flowControl.error(error);
          } else {
            final Logger logger = flowControl.logger();
            final ErrorFlowControl<T> errorFlowControl = new ErrorFlowControl<T>(logger);
            boolean thrown = false;
            Throwable error = this.error;
            for (final Block<? extends T, ? super Throwable> block : blocks) {
              logger.log(new DbgMessage("[executing] block: %s", new PrintIdentity(block)));
              try {
                if ((!errorFlowControl.isStopped() && !thrown) || (block instanceof Finally)) {
                  block.execute(error).apply(errorFlowControl);
                }
              } catch (final Exception e) {
                new IfInterrupt(e).throwException();
                logger.log(new WrnMessage(new LogMessage("original error overwritten by:"), e));
                thrown = true;
                error = e;
              }
            }
            errorFlowControl.applyResult(flowControl, error);
          }
        } else {
          super.execute(flowControl, message);
        }
      }
    }

    private class EndState extends MessageState {

      @Override
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        if (message == STOP) {
          if (blocks.isEmpty()) {
            flowControl.stop();
          } else {
            final Logger logger = flowControl.logger();
            final EndFlowControl<T> endFlowControl = new EndFlowControl<T>(logger);
            Throwable error = new TryThrowable();
            for (final Block<? extends T, ? super Throwable> block : blocks) {
              logger.log(new DbgMessage("[executing] block: %s", new PrintIdentity(block)));
              try {
                if (block instanceof Finally) {
                  block.execute(error).apply(endFlowControl);
                }
              } catch (final Exception e) {
                new IfInterrupt(e).throwException();
                logger.log(new WrnMessage(new LogMessage("original output overwritten by:"), e));
                error = e;
              }
            }
            endFlowControl.applyResult(flowControl, error);
          }
        } else {
          super.execute(flowControl, message);
        }
      }
    }

    private class AbortState extends MessageState {

      @Override
      void execute(@NotNull AwaitableFlowControl<T> flowControl, Object message) throws Exception {
        if (message == STOP) {
          if (blocks.isEmpty()) {
            flowControl.abort();
          } else {
            final Logger logger = flowControl.logger();
            final EndFlowControl<T> endFlowControl = new EndFlowControl<T>(logger);
            Throwable error = new TryThrowable();
            for (final Block<? extends T, ? super Throwable> block : blocks) {
              logger.log(new DbgMessage("[executing] block: %s", new PrintIdentity(block)));
              try {
                if (block instanceof Finally) {
                  block.execute(error).apply(endFlowControl);
                }
              } catch (final Exception e) {
                new IfInterrupt(e).throwException();
                logger.log(new WrnMessage(new LogMessage("original output overwritten by:"), e));
                error = e;
              }
            }
            endFlowControl.applyResult(flowControl, error);
          }
        } else {
          super.execute(flowControl, message);
        }
      }
    }
  }

  private static class TryThrowable extends Throwable {

    @Override
    public Throwable fillInStackTrace() {
      return this;
    }
  }
}
