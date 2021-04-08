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
import concoord.data.BufferFactory;
import concoord.data.DefaultBufferFactory;
import concoord.lang.StandardAwaitable.ExecutionControl;
import concoord.lang.StandardAwaitable.StandardFlowControl;
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

  private final BufferFactory<T> bufferFactory;

  public Streamed() {
    this(new DefaultBufferFactory<T>());
  }

  public Streamed(int initialCapacity) {
    this(new DefaultBufferFactory<T>(initialCapacity));
  }

  public Streamed(@NotNull BufferFactory<T> bufferFactory) {
    new IfNull("bufferFactory", bufferFactory).throwException();
    this.bufferFactory = bufferFactory;
  }

  @NotNull
  public StreamedAwaitable<T> on(@NotNull Scheduler scheduler) {
    return new StandardStreamedAwaitable<T>(scheduler, new StreamedControl<T>(bufferFactory));
  }

  public interface StreamedAwaitable<T> extends Awaitable<T>, Awaiter<T> {

    @NotNull
    Closeable asCloseable();
  }

  private static class StreamedControl<T> implements ExecutionControl<T>, Runnable {

    private final ConcurrentLinkedQueue<Object> inputQueue = new ConcurrentLinkedQueue<Object>();
    private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
    private final BufferFactory<T> bufferFactory;
    private Runnable controlCommand = new InitCommand();
    private State<T> controlState = new InitState();
    private StandardFlowControl<T> flowControl = new DummyFlowControl<T>();
    private Buffer<T> buffer;
    private Iterator<T> iterator;

    private StreamedControl(@NotNull BufferFactory<T> bufferFactory) {
      this.bufferFactory = bufferFactory;
    }

    public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
      this.flowControl = flowControl;
      return controlState.executeBlock(flowControl);
    }

    public void cancelExecution() {
      // do nothing, message flow is internally handled by the buffer
    }

    public void abortExecution(@NotNull Throwable error) {
      this.error.set(error);
    }

    public void run() {
      controlCommand.run();
    }

    private void error(@NotNull Throwable error) {
      controlState = new ErrorState(error);
      run();
    }

    private void end() {
      controlState = new EndState();
      run();
    }

    @Nullable
    private Throwable error() {
      return error.get();
    }

    @NotNull
    private ConcurrentLinkedQueue<Object> queue() {
      return inputQueue;
    }

    private interface State<T> {

      boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception;
    }

    private class InitCommand implements Runnable {

      public void run() {
        try {
          buffer = bufferFactory.create();
          iterator = buffer.iterator();
          controlState = new MessageState();
          controlCommand = new MessageCommand();
        } catch (Exception exception) {
          controlState = new ErrorState(exception);
          controlCommand = new FlowCommand();
        }
      }
    }

    private class FlowCommand implements Runnable {

      public void run() {
        flowControl.execute();
      }
    }

    private class MessageCommand extends FlowCommand {

      @SuppressWarnings("unchecked")
      public void run() {
        final Object message = inputQueue.poll();
        if (message != null) {
          buffer.add(message != NULL ? (T) message : null);
        }
        super.run();
      }
    }

    private class InitState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        buffer = bufferFactory.create();
        iterator = buffer.iterator();
        controlState = new MessageState();
        controlCommand = new MessageCommand();
        return controlState.executeBlock(flowControl);
      }
    }

    private class MessageState implements State<T> {

      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        final Iterator<T> iterator = StreamedControl.this.iterator;
        if (iterator.hasNext()) {
          flowControl.postOutput(iterator.next());
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
      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        if (!super.executeBlock(flowControl)) {
          flowControl.error(error);
        }
        return true;
      }
    }

    private class EndState extends MessageState {

      @Override
      public boolean executeBlock(@NotNull StandardFlowControl<T> flowControl) throws Exception {
        if (!super.executeBlock(flowControl)) {
          flowControl.stop();
        }
        return true;
      }
    }
  }

  private static class StandardStreamedAwaitable<T> extends StandardAwaitable<T> implements
      StreamedAwaitable<T> {

    private final Scheduler scheduler;
    private final StreamedControl<T> executionControl;

    public StandardStreamedAwaitable(@NotNull Scheduler scheduler,
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
    public Closeable asCloseable() {
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

    private final StandardStreamedAwaitable<?> awaitable;

    private CloseableAwaitable(@NotNull StandardStreamedAwaitable<?> awaitable) {
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
