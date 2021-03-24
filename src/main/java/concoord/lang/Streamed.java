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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.data.Buffer;
import concoord.data.Buffered;
import concoord.lang.BaseAwaitable.AwaitableFlowControl;
import concoord.lang.BaseAwaitable.ExecutionControl;
import concoord.util.assertion.IfNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Streamed<T> implements Task<T> {

  private static final Object NULL = new Object();
  private static final Object STOP = new Object();

  private final BufferFactory<T> factory;

  public Streamed() {
    this(new DefaultBufferFactory<T>());
  }

  public Streamed(int initialCapacity) {
    this(new CapacityBufferFactory<T>(initialCapacity));
  }

  public Streamed(@NotNull BufferFactory<T> factory) {
    new IfNull("factory", factory).throwException();
    this.factory = factory;
  }

  @NotNull
  public StreamedAwaitable<T> on(@NotNull Scheduler scheduler) {
    return new BaseStreamedAwaitable<T>(scheduler, new StreamedControl<T>(factory.create()));
  }

  public interface BufferFactory<T> {

    @NotNull
    Buffer<T> create();
  }

  public interface StreamedAwaitable<T> extends Awaitable<T>, Awaiter<T> {

    @NotNull
    Closeable asCloseable();
  }

  private static class DefaultBufferFactory<T> implements BufferFactory<T> {

    @NotNull
    public Buffer<T> create() {
      return new Buffered<T>();
    }
  }

  private static class CapacityBufferFactory<T> implements BufferFactory<T> {

    private final int initialCapacity;

    private CapacityBufferFactory(int initialCapacity) {
      this.initialCapacity = initialCapacity;
    }

    @NotNull
    public Buffer<T> create() {
      return new Buffered<T>(initialCapacity);
    }
  }

  private static class StreamedControl<T> implements ExecutionControl<T>, Runnable {

    private final ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
    private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
    private final MessageState message = new MessageState();
    private final Buffer<T> buffer;
    private final Iterator<T> inputs;
    private AwaitableFlowControl<T> flowControl;
    private State<T> state = message;

    private StreamedControl(@NotNull Buffer<T> buffer) {
      this.buffer = buffer;
      this.inputs = buffer.iterator();
    }

    public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
      this.flowControl = flowControl;
      return state.executeBlock(flowControl);
    }

    public void abortExecution(@NotNull Throwable error) {
      this.error.set(error);
    }

    @SuppressWarnings("unchecked")
    public void run() {
      final ConcurrentLinkedQueue<Object> inputs = this.queue;
      final Object message = inputs.peek();
      if ((message != null) && (message != STOP)) {
        inputs.remove();
        buffer.add(message != NULL ? (T) message : null);
      }
      if (flowControl != null) {
        flowControl.execute();
      }
    }

    private void error(@NotNull Throwable error) {
      queue.offer(STOP);
      state = new ErrorState(error);
      run();
    }

    private void end() {
      queue.offer(STOP);
      state = new EndState();
      run();
    }

    @Nullable
    private Throwable error() {
      return error.get();
    }

    @NotNull
    private ConcurrentLinkedQueue<Object> queue() {
      return queue;
    }

    private interface State<T> {

      boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception;
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (inputs.hasNext()) {
          flowControl.postOutput(inputs.next());
          return true;
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
        if (queue.peek() == STOP) {
          flowControl.error(error);
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull AwaitableFlowControl<T> flowControl) throws Exception {
        if (queue.peek() == STOP) {
          flowControl.stop();
          return true;
        }
        return super.executeBlock(flowControl);
      }
    }
  }

  private static class BaseStreamedAwaitable<T> extends BaseAwaitable<T> implements
      StreamedAwaitable<T> {

    private final Scheduler scheduler;
    private final StreamedControl<T> executionControl;

    public BaseStreamedAwaitable(@NotNull Scheduler scheduler,
        @NotNull StreamedControl<T> executionControl) {
      super(scheduler, executionControl);
      this.scheduler = scheduler;
      this.executionControl = executionControl;
    }

    public void message(T message) throws Exception {
      throwIfError();
      final StreamedControl<T> executionControl = this.executionControl;
      executionControl.queue().offer(message != null ? message : NULL);
      scheduler.scheduleLow(executionControl);
    }

    public void error(@NotNull Throwable error) throws Exception {
      throwIfError();
      scheduler.scheduleLow(new ErrorCommand(error));
    }

    public void end() throws Exception {
      throwIfError();
      scheduler.scheduleLow(new EndCommand());
    }

    @NotNull
    public CloseableAwaitable asCloseable() {
      return new CloseableAwaitable(this);
    }

    private void throwIfError() throws Exception {
      final Throwable error = executionControl.error();
      if (error != null) {
        if (error instanceof Exception) {
          throw (Exception) error;
        } else {
          throw new AbortException(error);
        }
      }
    }

    private class ErrorCommand implements Runnable {

      private final Throwable error;

      private ErrorCommand(@NotNull Throwable error) {
        this.error = error;
      }

      public void run() {
        executionControl.error(error);
      }
    }

    private class EndCommand implements Runnable {

      public void run() {
        executionControl.end();
      }
    }
  }

  private static class CloseableAwaitable implements Closeable {

    private final BaseStreamedAwaitable<?> awaitable;

    private CloseableAwaitable(@NotNull BaseStreamedAwaitable<?> awaitable) {
      this.awaitable = awaitable;
    }

    public void close() throws IOException {
      try {
        awaitable.end();
      } catch (final Exception e) {
        throw new StreamedIOException(e);
      }
    }
  }

  private static class StreamedIOException extends IOException {

    private StreamedIOException(@NotNull Throwable cause) {
      super(cause.toString());
      initCause(cause);
    }
  }
}
