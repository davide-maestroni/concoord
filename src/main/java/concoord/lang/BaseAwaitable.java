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
import concoord.concurrent.CombinedAwaiter;
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
  private final FlushState flush = new FlushState();
  private final InternalAwaiter awaiter = new InternalAwaiter();
  private final Scheduler scheduler;
  private final ExecutionControl<T> executionControl;
  private InternalFlowControl currentFlowControl;
  private Awaitable<? extends T> currentAwaitable;
  private Runnable currentState = read;
  private boolean hasOutputs;
  private boolean stopped;

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
    final InternalFlowControl flowControl = new InternalFlowControl(maxEvents, awaiter);
    scheduler.scheduleHigh(flowControl);
    return new BaseCancelable(flowControl);
  }

  @NotNull
  public Cancelable await(int maxEvents, @NotNull EventAwaiter<? super T> messageAwaiter,
      @NotNull EventAwaiter<? super Throwable> errorAwaiter,
      @NotNull EventAwaiter<? super Integer> endAwaiter) {
    return await(maxEvents, new CombinedAwaiter<T>(messageAwaiter, errorAwaiter, endAwaiter));
  }

  public void abort() {
    scheduler.scheduleHigh(new AbortCommand());
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

    void abort();

    void error(Throwable error);

    void schedule();

    int inputEvents();

    int outputEvents();

    boolean hasOutputs();

    @NotNull
    Logger logger();
  }

  public interface ExecutionControl<T> {

    boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;

    void abortExecution() throws Exception;
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

    public void end(int reason) {
    }
  }

  private class ReadState implements Runnable {

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      try {
        final Awaitable<? extends T> awaitable = currentAwaitable;
        if (awaitable != null) {
          currentState = write;
          awaitableLogger.log(new DbgMessage("[writing]"));
          awaitable.await(flowControl.outputEvents(), awaiter);
        } else if (stopped) {
          flowControl.sendEnd(Awaiter.DONE);
          nextFlowControl();
        } else {
          flowControl.resetPosts();
          if (!executionControl.executeBlock(flowControl)) {
            return;
          }
          if (flowControl.outputEvents() == 0) {
            nextFlowControl();
            return;
          }
          flowControl.schedule();
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new WrnMessage(new LogMessage("invocation failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.sendError(e);
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
          if (flowControl.outputEvents() == 0) {
            nextFlowControl();
          }
        }
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(new LogMessage("posting failed with an exception"), e)
        );
        new IfInterrupt(e).throwException();
        flowControl.sendError(e);
        nextFlowControl();
      }
    }
  }

  private class FlushState implements Runnable {

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
          if (flowControl.outputEvents() == 0) {
            nextFlowControl();
          } else {
            scheduler.scheduleLow(state);
          }
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
        flowControl.sendError(e);
        nextFlowControl();
      }
    }
  }

  private class StoppedState implements Runnable {

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      if (flowControl.outputEvents() != 0) {
        flowControl.sendEnd(Awaiter.DONE);
      }
      nextFlowControl();
    }
  }

  private class AbortedState implements Runnable {

    public void run() {
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      if (flowControl.outputEvents() != 0) {
        currentState = new StoppedState();
        flowControl.sendEnd(Awaiter.ABORTED);
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
      final InternalFlowControl flowControl = currentFlowControl;
      if (flowControl == null) {
        return;
      }
      flowControl.error(error);
    }
  }

  private class EndCommand implements Runnable {

    public void run() {
      currentAwaitable = null;
      if (stopped) {
        currentState = new StoppedState();
      } else {
        currentState = flush;
        final InternalFlowControl flowControl = currentFlowControl;
        if (flowControl == null) {
          return;
        }
      }
      currentState.run();
    }
  }

  private class AbortCommand implements Runnable {

    public void run() {
      stopped = true;
      currentState = new AbortedState();
      awaitableLogger.log(new InfMessage("[aborted]"));
      try {
        if (currentAwaitable != null) {
          currentAwaitable.abort();
          currentAwaitable = null;
        }
        executionControl.abortExecution();
        currentState.run();
      } catch (final Exception e) {
        awaitableLogger.log(new ErrMessage(new LogMessage("failed to cancel execution"), e));
        new IfInterrupt(e).throwException();
        final InternalFlowControl flowControl = currentFlowControl;
        if ((flowControl != null) && (flowControl.outputEvents() != 0)) {
          flowControl.sendError(e);
        }
      }
      nextFlowControl();
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

    public void end(int reason) {
      if (reason == Awaiter.ABORTED) {
        scheduler.scheduleLow(new AbortCommand());
      } else {
        scheduler.scheduleLow(end);
      }
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
        sendError(e);
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

    public void abort() {
      scheduler.scheduleLow(new AbortCommand());
    }

    public void error(Throwable error) {
      if (outputEvents != 0) {
        sendError(error);
      }
      nextFlowControl();
    }

    public void schedule() {
      scheduler.scheduleLow(state);
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
      stopped = true;
      currentState = new StoppedState();
      awaitableLogger.log(new InfMessage(new LogMessage("[failed] with error:"), error));
      try {
        executionControl.abortExecution();
      } catch (final Exception e) {
        awaitableLogger.log(
            new ErrMessage(new LogMessage("ignoring exception during failure"), e)
        );
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

    private void sendEnd(int reason) {
      awaitableLogger.log(new InfMessage("[ended]"));
      try {
        flowAwaiter.end(reason);
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
        sendError(e);
      }
    }

    private class CancelCommand implements Runnable {

      public void run() {
        final InternalFlowControl flowControl = currentFlowControl;
        if (flowControl == InternalFlowControl.this) {
          if (outputEvents != 0) {
            sendEnd(Awaiter.CANCELED); // TODO: 19/03/21 remove?
          }
          nextFlowControl();
        } else {
          flowControls.remove(InternalFlowControl.this);
        }
      }
    }
  }
}
