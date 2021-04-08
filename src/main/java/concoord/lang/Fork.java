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
import concoord.lang.StandardAwaitable.ExecutionControl;
import concoord.lang.StandardAwaitable.StandardFlowControl;
import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import java.util.Iterator;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jetbrains.annotations.NotNull;

public class Fork<T> implements Task<T> {

  private static final Object NULL = new Object();

  private final WeakHashMap<ForkControl, Void> controls = new WeakHashMap<ForkControl, Void>();
  private final ConcurrentLinkedQueue<Object> inputQueue = new ConcurrentLinkedQueue<Object>();
  private final Trampoline trampoline = new Trampoline();
  private final ReadRunnable readState = new ReadRunnable();
  private final WriteRunnable writeState = new WriteRunnable();
  private final Awaitable<? extends T> awaitable;
  private final Buffer<T> buffer;
  private Runnable taskState = readState;
  private int maxEvents;
  private int eventCount;

  public Fork(@NotNull Awaitable<? extends T> awaitable, @NotNull Buffer<T> buffer) {
    this(1, awaitable, buffer);
  }

  public Fork(int maxEvents, @NotNull Awaitable<? extends T> awaitable, @NotNull Buffer<T> buffer) {
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
    return new Reschedule<T>(
        new StandardAwaitable<T>(trampoline, new ForkControl())
    ).on(scheduler);
  }

  private interface State<T> {

    boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception;
  }

  private class ReadRunnable implements Runnable {

    public void run() {
      int events = maxEvents;
      maxEvents = 0;
      for (ForkControl forkControl : controls.keySet()) {
        final int inputEvents = forkControl.inputEvents();
        if (events >= 0) {
          if (inputEvents < 0) {
            events = inputEvents;
          } else {
            events = Math.max(events, inputEvents);
          }
        }
      }
      if (events > 0) {
        taskState = writeState;
        eventCount = events;
        awaitable.await(events, new ForkAwaiter());
      }
    }
  }

  private class WriteRunnable implements Runnable {

    @SuppressWarnings("unchecked")
    public void run() {
      final Object message = inputQueue.poll();
      if (message != null) {
        buffer.add((T) message);
        for (ForkControl forkControl : controls.keySet()) {
          forkControl.run();
        }
        if ((eventCount > 0) && (--eventCount == 0)) {
          taskState = readState;
        }
      }
    }
  }

  private class ErrorRunnable extends WriteRunnable {

    private final Throwable error;

    private ErrorRunnable(@NotNull Throwable error) {
      this.error = error;
    }

    @Override
    public void run() {
      final Object message = inputQueue.peek();
      if (message == null) {
        for (ForkControl forkControl : controls.keySet()) {
          forkControl.error(error);
          forkControl.run();
        }
      } else {
        super.run();
      }
    }
  }

  private class EndRunnable extends WriteRunnable {

    @Override
    public void run() {
      final Object message = inputQueue.peek();
      if (message == null) {
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

    private final MessageCommand messageCommand = new MessageCommand();

    public void message(T message) {
      inputQueue.offer(message != null ? message : NULL);
      trampoline.scheduleLow(messageCommand);
    }

    public void error(@NotNull Throwable error) {
      trampoline.scheduleLow(new ErrorCommand(error));
    }

    public void end() {
      trampoline.scheduleLow(new EndCommand());
    }

    private class MessageCommand implements Runnable {

      public void run() {
        taskState.run();
      }
    }

    private class ErrorCommand implements Runnable {

      private final Throwable error;

      private ErrorCommand(@NotNull Throwable error) {
        this.error = error;
      }

      public void run() {
        taskState = new ErrorRunnable(error);
        taskState.run();
      }
    }

    private class EndCommand implements Runnable {

      public void run() {
        taskState = new EndRunnable();
        taskState.run();
      }
    }
  }

  private class ForkControl implements ExecutionControl<T>, Runnable {

    private State<T> controlState = new InitState();
    private StandardFlowControl<T> flowControl = new DummyFlowControl<T>();
    private Iterator<T> iterator;

    public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      // do nothing, message flow is internally handled by the buffer
    }

    public void abortExecution(@NotNull Throwable error) {
      controls.remove(this);
    }

    public void run() {
      flowControl.execute();
    }

    private void error(@NotNull Throwable error) {
      controlState = new ErrorState(error);
    }

    private void stop() {
      controlState = new EndState();
    }

    private int inputEvents() {
      return flowControl.inputEvents();
    }

    private class InitState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        controls.put(ForkControl.this, null);
        iterator = buffer.iterator();
        controlState = new ReadState();
        return controlState.executeBlock(flowControl);
      }
    }

    private class ReadState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        final Iterator<T> iterator = ForkControl.this.iterator;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
          return true;
        } else {
          taskState.run();
        }
        return false;
      }
    }

    private class ErrorState implements State<T> {

      private final Throwable error;

      private ErrorState(@NotNull Throwable error) {
        this.error = error;
      }

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        final Iterator<T> iterator = ForkControl.this.iterator;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
        } else {
          flowControl.error(error);
        }
        return true;
      }
    }

    private class EndState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        ForkControl.this.flowControl = flowControl;
        final Iterator<T> iterator = ForkControl.this.iterator;
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
