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
import concoord.concurrent.Trampoline;
import concoord.data.Buffer;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Fork<T> implements Task<T> {

  private static final Object NULL = new Object();
  private static final Object STOP = new Object();

  private final WeakHashMap<ForkControl, Void> controls = new WeakHashMap<ForkControl, Void>();
  private final ConcurrentLinkedQueue<Object> inputs = new ConcurrentLinkedQueue<Object>();
  private final Trampoline trampoline = new Trampoline();
  private final ReadState read = new ReadState();
  private final WriteState write = new WriteState();
  private final Awaitable<T> awaitable;
  private final Buffer<T> buffer;
  private int maxEvents;
  private int inputEvents;
  private Runnable state = read;

  public Fork(@NotNull Awaitable<T> awaitable, @NotNull Buffer<T> buffer) {
    this(1, awaitable, buffer);
  }

  public Fork(int maxEvents, @NotNull Awaitable<T> awaitable, @NotNull Buffer<T> buffer) {
    new IfSomeOf(
        new IfNull("awaitable", awaitable),
        new IfNull("buffer", buffer)
    ).throwException();
    this.maxEvents = maxEvents;
    this.awaitable = awaitable;
    this.buffer = buffer;
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return new Reschedule<T>(new BaseAwaitable<T>(trampoline, new ForkControl()))
        .on(scheduler);
  }

  private interface ForkState<T> {

    boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
  }

  private class ReadState implements Runnable {

    public void run() {
      int events = maxEvents;
      maxEvents = 0;
      for (ForkControl forkControl : controls.keySet()) {
        int inputEvents = forkControl.inputEvents();
        if (events >= 0) {
          if (inputEvents < 0) {
            events = inputEvents;
          } else {
            events = Math.max(events, inputEvents);
          }
        }
      }
      if (events > 0) {
        state = write;
        inputEvents = events;
        awaitable.await(events, new ForkAwaiter());
      }
    }
  }

  private class WriteState implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      final Object message = inputs.poll();
      if (message != null) {
        buffer.add((T) message);
        for (ForkControl forkControl : controls.keySet()) {
          forkControl.run();
        }
        if (--inputEvents == 0) {
          state = read;
        }
      }
    }
  }

  private class ErrorState extends WriteState {

    private final Throwable error;

    private ErrorState(@NotNull Throwable error) {
      this.error = error;
    }

    @Override
    public void run() {
      final Object message = inputs.peek();
      if (message == STOP) {
        for (ForkControl forkControl : controls.keySet()) {
          forkControl.error(error);
          forkControl.run();
        }
      } else {
        super.run();
      }
    }
  }

  private class EndState extends WriteState {

    @Override
    public void run() {
      final Object message = inputs.peek();
      if (message == STOP) {
        for (ForkControl forkControl : controls.keySet()) {
          forkControl.stop();
          forkControl.run();
        }
      } else {
        super.run();
      }
    }
  }

  private class ForkAwaiter implements Awaiter<T> {

    private final FlowCommand flow = new FlowCommand();

    public void message(T message) {
      inputs.offer(message != null ? message : NULL);
      trampoline.scheduleLow(flow);
    }

    public void error(@NotNull Throwable error) {
      trampoline.scheduleLow(new ErrorCommand(error));
    }

    public void end() {
      trampoline.scheduleLow(new EndCommand());
    }

    private class FlowCommand implements Runnable {

      public void run() {
        state.run();
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
        state.run();
      }
    }

    private class EndCommand implements Runnable {

      public void run() {
        inputs.offer(STOP);
        state = new EndState();
        state.run();
      }
    }
  }

  private class ForkControl implements ExecutionControl<T>, Runnable {

    private ForkState<T> forkState = new InitForkState();
    private AwaitableFlowControl<T> flowControl;
    private Iterator<T> iterator;

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      return forkState.executeBlock(flowControl);
    }

    public void abortExecution(@NotNull Throwable error) {
      controls.remove(this);
    }

    public void run() {
      if (flowControl != null) {
        flowControl.execute();
      }
    }

    private void error(@NotNull Throwable error) {
      forkState = new ErrorForkState(error);
    }

    private void stop() {
      forkState = new EndForkState();
    }

    private int inputEvents() {
      if (flowControl != null) {
        return flowControl.inputEvents();
      }
      return 0;
    }

    private class InitForkState implements ForkState<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        controls.put(ForkControl.this, null);
        iterator = buffer.iterator();
        forkState = new ReadForkState();
        return forkState.executeBlock(flowControl);
      }
    }

    private class ReadForkState implements ForkState<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
          return true;
        } else {
          state.run();
        }
        return false;
      }
    }

    private class ErrorForkState implements ForkState<T> {

      private final Throwable error;

      private ErrorForkState(@NotNull Throwable error) {
        this.error = error;
      }

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
        } else {
          flowControl.error(error);
        }
        return true;
      }
    }

    private class EndForkState implements ForkState<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
        } else {
          flowControl.stop();
        }
        return true;
      }
    }
  }
}
