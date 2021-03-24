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

import concoord.concurrent.AbortException;
import concoord.concurrent.Awaitable;
import concoord.concurrent.Awaiter;
import concoord.concurrent.CancelException;
import concoord.concurrent.Cancelable;
import concoord.concurrent.CombinedAwaiter;
import concoord.concurrent.EndAwaiter;
import concoord.concurrent.EventAwaiter;
import concoord.concurrent.Scheduler;
import concoord.flow.FlowControl;
import concoord.logging.DbgMessage;
import concoord.logging.ErrMessage;
import concoord.logging.InfMessage;
import concoord.logging.LogMessage;
import concoord.logging.Logger;
import concoord.logging.PrintIdentity;
import concoord.logging.WrnMessage;
import concoord.util.assertion.IfInterrupt;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import concoord.util.collection.CircularQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public class BaseAwaitable<T> implements Awaitable<T> {

  private static final Object NULL = new Object();

  private final Logger awaitableLogger = new Logger(Awaitable.class, this);
  private final CircularQueue<InternalFlowControl> flowControls = new CircularQueue<InternalFlowControl>();
  private final ConcurrentLinkedQueue<Object> messages = new ConcurrentLinkedQueue<Object>();
  private final StateCommand state = new StateCommand();
  private final EndCommand end = new EndCommand();
  private final ReadState read = new ReadState();
  private final WriteState write = new WriteState();
  private final FlushWriteState flush = new FlushWriteState();
  private final InternalAwaiter awaiter = new InternalAwaiter();
  private final Scheduler scheduler;
  private final ExecutionControl<T> executionControl;
  private InternalFlowControl currentFlowControl;
  private Awaitable<? extends T> currentAwaitable;
  private Runnable currentState = read;
  private boolean hasOutputs;
  private boolean stopped;
  private boolean aborted;

  public BaseAwaitable(@NotNull Scheduler scheduler,
      @NotNull ExecutionControl<T> executionControl) {
    new IfSomeOf(
        new IfNull("scheduler", scheduler),
        new IfNull("executionControl", executionControl)
    ).throwException();
    this.scheduler = scheduler;
    this.executionControl = executionControl;
    awaitableLogger.log(new InfMessage("[scheduled] on: %s", new PrintIdentity(scheduler)));
  }

  @NotNull
  public Cancelable await(int maxEvents) {
    return await(maxEvents, new DummyAwaiter<T>());
  }

  @NotNull
  public Cancelable await(int maxEvents, @NotNull Awaiter<? super T> awaiter) {
    new IfNull("awaiter", awaiter).throwException();
    if (maxEvents == 0) {
      awaitableLogger.log(new WrnMessage("await() called with 0 events"));
      // awaiter will never be called
      return new DummyCancelable();
    }
    final InternalFlowControl flowControl = new InternalFlowControl(maxEvents, awaiter);
    scheduler.scheduleHigh(flowControl);
    return new BaseCancelable(flowControl);
  }

  @NotNull
  public Cancelable await(int maxEvents, @NotNull EventAwaiter<? super T> messageAwaiter,
      @NotNull EventAwaiter<? super Throwable> errorAwaiter, @NotNull EndAwaiter endAwaiter) {
    return await(maxEvents, new CombinedAwaiter<T>(messageAwaiter, errorAwaiter, endAwaiter));
  }

  public void abort() {
    scheduler.scheduleHigh(new AbortCommand());
  }

  private void abortExecution(@NotNull Throwable error) {
    try {
      if (currentAwaitable != null) {
        currentAwaitable.abort();
        currentAwaitable = null;
      }
      executionControl.abortExecution(error);
    } catch (final Exception e) {
      awaitableLogger.log(
          new ErrMessage(new LogMessage("failed to abort execution (ignoring)"), e)
      );
      new IfInterrupt(e).throwException();
    }
  }

  private void nextFlowControl() {
    currentFlowControl = flowControls.poll();
    if (currentFlowControl != null) {
      scheduler.scheduleLow(currentFlowControl);
    } else {
      awaitableLogger.log(new InfMessage("[settled]"));
    }
  }

  public interface AwaitableFlowControl<T> extends FlowControl<T> {

    void error(@NotNull Throwable error);

    void execute();

    int inputEvents();

    int outputEvents();

    boolean hasOutputs();

    @NotNull
    Logger logger();
  }

  public interface ExecutionControl<T> {

    boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;

    void abortExecution(@NotNull Throwable error) throws Exception;
  }

  private static class BaseCancelable implements Cancelable {

    private final Cancelable cancelable;

    private BaseCancelable(@NotNull Cancelable cancelable) {
      this.cancelable = cancelable;
    }

    public boolean isError() {
      return cancelable.isError();
    }

    public boolean isDone() {
      return cancelable.isDone();
    }

    public void cancel() {
      cancelable.cancel();
    }
  }

  private static class DummyAwaiter<T> implements Awaiter<T> {

    public void message(T message) {
    }

    public void error(@NotNull Throwable error) {
    }

    public void end() {
    }
  }

  private static class DummyCancelable implements Cancelable {

    public boolean isError() {
      return false;
    }

    public boolean isDone() {
      return false;
    }

    public void cancel() {
    }
  }

  private class ReadState implements Runnable {

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      try {
        flowControl.resetPosts();
        if (!executionControl.executeBlock(flowControl)) {
          return;
        }
        final Awaitable<? extends T> awaitable = currentAwaitable;
        if (awaitable != null) {
          currentState = write;
          awaitableLogger.log(new DbgMessage("[writing]"));
          awaitable.await(flowControl.outputEvents(), awaiter);
        } else if (stopped) {
          currentState = new EndState();
          awaitableLogger.log(new DbgMessage("[ending]"));
          currentState.run();
        } else if ((flowControl.outputEvents() == 0) || aborted) {
          nextFlowControl();
        } else {
          scheduler.scheduleLow(state);
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new WrnMessage(new LogMessage("invocation failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.error(e);
        nextFlowControl();
      }
    }
  }

  private class WriteState implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      try {
        final Object message = messages.poll();
        if (message != null) {
          flowControl.resetPosts();
          flowControl.postOutput(message != NULL ? (T) message : null);
          if ((flowControl.outputEvents() == 0) || aborted) {
            nextFlowControl();
          }
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(new LogMessage("posting failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.error(e);
        nextFlowControl();
      }
    }
  }

  private class FlushWriteState implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      try {
        final Object message = messages.poll();
        if (message != null) {
          flowControl.resetPosts();
          flowControl.postOutput(message != NULL ? (T) message : null);
          if ((flowControl.outputEvents() == 0) || aborted) {
            nextFlowControl();
          } else {
            scheduler.scheduleLow(state);
          }
        } else if (stopped) {
          currentState = new EndState();
          awaitableLogger.log(new DbgMessage("[ending]"));
          currentState.run();
        } else {
          currentState = read;
          awaitableLogger.log(new DbgMessage("[reading]"));
          currentState.run();
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(new LogMessage("posting failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.error(e);
        nextFlowControl();
      }
    }
  }

  private class FlushErrorState implements Runnable {

    private final Throwable error;

    private FlushErrorState(@NotNull Throwable error) {
      this.error = error;
    }

    @SuppressWarnings("unchecked")
    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      try {
        final Object message = messages.poll();
        if (message != null) {
          flowControl.resetPosts();
          flowControl.postOutput(message != NULL ? (T) message : null);
          if ((flowControl.outputEvents() == 0) || aborted) {
            nextFlowControl();
          } else {
            scheduler.scheduleLow(state);
          }
        } else {
          currentState = new ErrorState(error);
          awaitableLogger.log(new DbgMessage("[failing]"));
          currentState.run();
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(new LogMessage("posting failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.error(e);
        nextFlowControl();
      }
    }
  }

  private class EndState implements Runnable {

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      if (flowControl.outputEvents() != 0) {
        flowControl.sendEnd();
      }
      nextFlowControl();
    }
  }

  private class ErrorState implements Runnable {

    private final Throwable error;

    private ErrorState(@NotNull Throwable error) {
      this.error = error;
    }

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      if (flowControl.outputEvents() != 0) {
        flowControl.sendError(error);
      }
      nextFlowControl();
    }
  }

  private class StateCommand implements Runnable {

    public void run() {
      currentState.run();
    }
  }

  private class ErrorCommand implements Runnable {

    private final Throwable error;

    private ErrorCommand(@NotNull Throwable error) {
      this.error = error;
    }

    public void run() {
      currentAwaitable = null;
      if (aborted) {
        return;
      }
      currentState = new FlushErrorState(error);
      currentState.run();
    }
  }

  private class EndCommand implements Runnable {

    public void run() {
      currentAwaitable = null;
      if (aborted) {
        return;
      }
      currentState = flush;
      currentState.run();
    }
  }

  private class AbortCommand implements Runnable {

    private final AbortException error = new AbortException();

    public void run() {
      aborted = true;
      currentState = new ErrorState(error);
      awaitableLogger.log(new InfMessage("[aborting]"));
      abortExecution(error);
      currentState.run();
    }
  }

  private class InternalAwaiter implements Awaiter<T> {

    public void message(T message) {
      messages.offer(message != null ? message : NULL);
      scheduler.scheduleLow(state);
    }

    public void error(@NotNull Throwable error) {
      scheduler.scheduleLow(new ErrorCommand(error));
    }

    public void end() {
      scheduler.scheduleLow(end);
    }
  }

  private class InternalFlowControl implements AwaitableFlowControl<T>, Cancelable, Runnable {

    private static final int ERROR = -1;
    private static final int RUNNING = 0;
    private static final int DONE = 1;

    private final Logger flowLogger = new Logger(FlowControl.class, this);
    private final AtomicInteger status = new AtomicInteger();
    private final Awaiter<? super T> flowAwaiter;
    private final int totEvents;
    private int inputEvents = 1;
    private int outputEvents;
    private int posts;

    private InternalFlowControl(int maxEvents, @NotNull Awaiter<? super T> flowAwaiter) {
      this.totEvents = maxEvents;
      this.outputEvents = maxEvents;
      this.flowAwaiter = flowAwaiter;
      flowLogger.log(new DbgMessage("[initialized]"));
    }

    public void postOutput(T message) {
      if (++posts > 1) {
        throw new IllegalStateException("multiple outputs posted by the result");
      }
      if (totEvents >= 0) {
        flowLogger.log(new DbgMessage("[posting] new message: %d", totEvents - outputEvents));
        --outputEvents;
      } else {
        flowLogger.log(new DbgMessage("[posting] new message"));
      }
      try {
        flowAwaiter.message(message);
        hasOutputs = true;
      } catch (final Exception e) {
        new IfInterrupt(e).throwException();
        error(e);
      }
    }

    public void postOutput(Awaitable<? extends T> awaitable) {
      if (++posts > 1) {
        throw new IllegalStateException("multiple outputs posted by the result");
      }
      if (awaitable != null) {
        flowLogger.log(
            new DbgMessage("[posting] new awaitable: %s", new PrintIdentity(awaitable))
        );
        currentAwaitable = awaitable;
      } else {
        flowLogger.log(new WrnMessage("[posting] awaitable ignored: null"));
      }
    }

    public void nextInputs(int maxEvents) {
      flowLogger.log(new DbgMessage("[limiting] event number: %d", maxEvents));
      inputEvents = maxEvents;
    }

    public void stop() {
      stopped = true;
      flowLogger.log(new DbgMessage("[stopped]"));
      awaitableLogger.log(new InfMessage("[complete]"));
    }

    public void error(@NotNull Throwable error) {
      aborted = true;
      currentState = new ErrorState(error);
      awaitableLogger.log(new DbgMessage("[failing]"));
      abortExecution(error);
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      if (flowControl.outputEvents() != 0) {
        flowControl.sendError(error);
      }
    }

    public void execute() {
      state.run();
    }

    public int inputEvents() {
      return inputEvents;
    }

    public int outputEvents() {
      return outputEvents;
    }

    public boolean hasOutputs() {
      return hasOutputs;
    }

    @NotNull
    public Logger logger() {
      return awaitableLogger;
    }

    public boolean isError() {
      return status.get() == ERROR;
    }

    public boolean isDone() {
      return status.get() == DONE;
    }

    public void cancel() {
      if (status.get() != RUNNING) {
        return;
      }
      scheduler.scheduleHigh(new CancelCommand());
    }

    public void run() {
      final InternalFlowControl thisFlowControl = InternalFlowControl.this;
      if (currentFlowControl == null) {
        currentFlowControl = thisFlowControl;
      }
      if (currentFlowControl != thisFlowControl) {
        flowControls.offer(thisFlowControl);
        return;
      }
      awaitableLogger.log(new DbgMessage("[reading]"));
      currentState.run();
    }

    private void resetPosts() {
      posts = 0;
    }

    private void sendError(@NotNull Throwable error) {
      if (!(error instanceof CancelException)) {
        awaitableLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
        try {
          executionControl.abortExecution(error);
        } catch (final Exception e) {
          awaitableLogger.log(
              new ErrMessage(new LogMessage("exception during failure (ignored)"), e)
          );
        }
      }
      try {
        flowAwaiter.error(error);
        status.set(ERROR);
        hasOutputs = true;
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(
                new LogMessage(
                    "failed to notify error to awaiter: %s",
                    new PrintIdentity(flowAwaiter)
                ),
                e
            )
        );
        new IfInterrupt(e).throwException();
      }
    }

    private void sendEnd() {
      awaitableLogger.log(new InfMessage("[ended]"));
      try {
        flowAwaiter.end();
        status.set(DONE);
        hasOutputs = true;
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(
                new LogMessage("failed to notify end to awaiter: %s",
                    new PrintIdentity(flowAwaiter)),
                e
            )
        );
        new IfInterrupt(e).throwException();
        error(e);
      }
    }

    private class CancelCommand implements Runnable {

      private final CancelException error = new CancelException();

      public void run() {
        final InternalFlowControl flowControl = currentFlowControl;
        if (flowControl == InternalFlowControl.this) {
          if (outputEvents != 0) {
            sendError(error);
          }
          nextFlowControl();
        } else {
          flowControls.remove(InternalFlowControl.this);
        }
      }
    }
  }
}
