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
package concoord.scheduling.output;

import concoord.data.Buffer;
import concoord.data.BufferFactory;
import concoord.data.DefaultBufferFactory;
import concoord.lang.Parallel.InputChannel;
import concoord.lang.Parallel.OutputChannel;
import concoord.util.assertion.IfNull;
import concoord.util.collection.CircularQueue;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class OrderedOutput<M> implements OutputControl<M> {

  private final CircularQueue<OrderedInputChannel<M>> inputChannels =
      new CircularQueue<OrderedInputChannel<M>>();
  private final BufferFactory<M> bufferFactory;
  private OrderedOutputChannel<M> outputChannel;

  public OrderedOutput() {
    this.bufferFactory = new DefaultBufferFactory<M>();
  }

  public OrderedOutput(@NotNull BufferFactory<M> bufferFactory) {
    new IfNull("bufferFactory", bufferFactory).throwException();
    this.bufferFactory = bufferFactory;
  }

  @NotNull
  public InputChannel<M> outputBufferInput() throws Exception {
    final OrderedInputChannel<M> inputChannel =
        new OrderedInputChannel<M>(bufferFactory.create());
    inputChannels.offer(inputChannel);
    return inputChannel;
  }

  @NotNull
  public OutputChannel<M> outputBufferOutput() {
    if (outputChannel == null) {
      outputChannel = new OrderedOutputChannel<M>(inputChannels);
    }
    return outputChannel;
  }

  private static class OrderedInputChannel<M> implements InputChannel<M> {

    private final Buffer<M> buffer;
    private final Iterator<M> iterator;
    private boolean closed;

    private OrderedInputChannel(@NotNull Buffer<M> buffer) {
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

  private static class OrderedOutputChannel<M> implements OutputChannel<M> {

    private final CircularQueue<OrderedInputChannel<M>> inputChannels;

    private OrderedOutputChannel(@NotNull CircularQueue<OrderedInputChannel<M>> inputChannels) {
      this.inputChannels = inputChannels;
    }

    public boolean hasNext() {
      final CircularQueue<OrderedInputChannel<M>> inputChannels = this.inputChannels;
      while (!inputChannels.isEmpty()) {
        final OrderedInputChannel<M> inputChannel = inputChannels.peek();
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
