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
import concoord.concurrent.Scheduler;
import concoord.concurrent.Task;
import concoord.data.Buffer;
import concoord.data.BufferFactory;
import concoord.data.DefaultBufferFactory;
import concoord.lang.Parallel.BufferControl;
import concoord.lang.Parallel.InputChannel;
import concoord.lang.Parallel.OutputChannel;
import concoord.lang.Parallel.SchedulingStrategyFactory;
import concoord.util.assertion.IfNull;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class ParallelAny<T, M> implements Task<T> {

  private final Parallel<T, M> task;

  public ParallelAny(@NotNull Awaitable<M> awaitable,
      @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
    this(awaitable, new DefaultBufferFactory<T>(), strategyFactory);
  }

  public ParallelAny(@NotNull Awaitable<M> awaitable, @NotNull BufferFactory<T> bufferFactory,
      @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
    new IfNull("bufferFactory", bufferFactory).throwException();
    this.task = new Parallel<T, M>(
        awaitable,
        new AnyBufferControl<T>(bufferFactory),
        strategyFactory
    );
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return task.on(scheduler);
  }

  private static class AnyBufferControl<M> implements BufferControl<M> {

    private final BufferFactory<M> factory;
    private Buffer<M> buffer;
    private AnyInputChannel<M> inputChannel;
    private AnyOutputChannel<M> outputChannel;

    private AnyBufferControl(@NotNull BufferFactory<M> factory) {
      this.factory = factory;
    }

    @NotNull
    public InputChannel<M> inputChannel() throws Exception {
      if (inputChannel == null) {
        if (buffer == null) {
          buffer = factory.create();
          new IfNull("buffer", buffer).throwException();
        }
        inputChannel = new AnyInputChannel<M>(buffer);
      }
      return inputChannel;
    }

    @NotNull
    public OutputChannel<M> outputChannel() throws Exception {
      if (outputChannel == null) {
        if (buffer == null) {
          buffer = factory.create();
        }
        outputChannel = new AnyOutputChannel<M>(buffer.iterator());
      }
      return outputChannel;
    }
  }

  private static class AnyInputChannel<M> implements InputChannel<M> {

    private final Buffer<M> buffer;

    private AnyInputChannel(@NotNull Buffer<M> buffer) {
      this.buffer = buffer;
    }

    public void push(M message) {
      buffer.add(message);
    }

    public void close() {
      // do nothing
    }
  }

  private static class AnyOutputChannel<M> implements OutputChannel<M> {

    private final Iterator<M> iterator;

    private AnyOutputChannel(@NotNull Iterator<M> iterator) {
      this.iterator = iterator;
    }

    public boolean hasNext() {
      return iterator.hasNext();
    }

    public M next() {
      return iterator.next();
    }
  }
}
