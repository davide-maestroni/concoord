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
import concoord.util.collection.CircularQueue;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class ParallelAnd<T, M> implements Task<T> {

  private final Parallel<T, M> task;

  public ParallelAnd(@NotNull Awaitable<M> awaitable,
      @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
    this(awaitable, new DefaultBufferFactory<T>(), strategyFactory);
  }

  public ParallelAnd(@NotNull Awaitable<M> awaitable, @NotNull BufferFactory<T> bufferFactory,
      @NotNull SchedulingStrategyFactory<? extends T, ? super M> strategyFactory) {
    new IfNull("bufferFactory", bufferFactory).throwException();
    this.task = new Parallel<T, M>(
        awaitable,
        new AndBufferControl<T>(bufferFactory),
        strategyFactory
    );
  }

  @NotNull
  public Awaitable<T> on(@NotNull Scheduler scheduler) {
    return task.on(scheduler);
  }

  private static class AndBufferControl<M> implements BufferControl<M> {

    private final CircularQueue<AndInputChannel<M>> inputChannels =
        new CircularQueue<AndInputChannel<M>>();
    private final BufferFactory<M> factory;
    private AndOutputChannel<M> outputChannel;

    private AndBufferControl(@NotNull BufferFactory<M> factory) {
      this.factory = factory;
    }

    @NotNull
    public InputChannel<M> inputChannel() throws Exception {
      final AndInputChannel<M> inputChannel = new AndInputChannel<M>(factory.create());
      inputChannels.offer(inputChannel);
      return inputChannel;
    }

    @NotNull
    public OutputChannel<M> outputChannel() {
      if (outputChannel == null) {
        outputChannel = new AndOutputChannel<M>(inputChannels);
      }
      return outputChannel;
    }
  }

  private static class AndInputChannel<M> implements InputChannel<M> {

    private final Buffer<M> buffer;
    private final Iterator<M> iterator;
    private boolean closed;

    private AndInputChannel(@NotNull Buffer<M> buffer) {
      this.buffer = buffer;
      this.iterator = buffer.iterator();
    }

    public void push(M message) {
      buffer.add(message);
    }

    public void close() {
      closed = true;
    }

    private boolean hasNext() {
      return iterator.hasNext();
    }

    private M next() {
      return iterator.next();
    }

    private boolean isClosed() {
      return closed;
    }
  }

  private static class AndOutputChannel<M> implements OutputChannel<M> {

    private final CircularQueue<AndInputChannel<M>> inputChannels;

    private AndOutputChannel(@NotNull CircularQueue<AndInputChannel<M>> inputChannels) {
      this.inputChannels = inputChannels;
    }

    public boolean hasNext() {
      final CircularQueue<AndInputChannel<M>> inputChannels = this.inputChannels;
      while (!inputChannels.isEmpty()) {
        final AndInputChannel<M> inputChannel = inputChannels.peek();
        if (!inputChannel.hasNext() && inputChannel.isClosed()) {
          inputChannels.remove();
        } else {
          break;
        }
      }
      if (!inputChannels.isEmpty()) {
        return inputChannels.peek().hasNext();
      }
      return false;
    }

    @SuppressWarnings("ConstantConditions")
    public M next() {
      return inputChannels.peek().next();
    }
  }
}
