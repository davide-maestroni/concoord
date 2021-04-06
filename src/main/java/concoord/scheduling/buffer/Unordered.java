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
package concoord.scheduling.buffer;

import concoord.data.Buffer;
import concoord.data.BufferFactory;
import concoord.data.DefaultBufferFactory;
import concoord.lang.Parallel.BufferControl;
import concoord.lang.Parallel.InputChannel;
import concoord.lang.Parallel.OutputChannel;
import concoord.util.assertion.IfNull;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;

public class Unordered<M> implements BufferControl<M> {

  private final BufferFactory<M> bufferFactory;
  private Buffer<M> buffer;
  private UnorderedInputChannel<M> inputChannel;
  private UnorderedOutputChannel<M> outputChannel;

  public Unordered() {
    this.bufferFactory = new DefaultBufferFactory<M>();
  }

  public Unordered(@NotNull BufferFactory<M> bufferFactory) {
    new IfNull("bufferFactory", bufferFactory).throwException();
    this.bufferFactory = bufferFactory;
  }

  @NotNull
  public InputChannel<M> inputChannel() throws Exception {
    if (inputChannel == null) {
      if (buffer == null) {
        buffer = bufferFactory.create();
        new IfNull("buffer", buffer).throwException();
      }
      inputChannel = new UnorderedInputChannel<M>(buffer);
    }
    return inputChannel;
  }

  @NotNull
  public OutputChannel<M> outputChannel() throws Exception {
    if (outputChannel == null) {
      if (buffer == null) {
        buffer = bufferFactory.create();
      }
      outputChannel = new UnorderedOutputChannel<M>(buffer.iterator());
    }
    return outputChannel;
  }

  private static class UnorderedInputChannel<M> implements InputChannel<M> {

    private final Buffer<M> buffer;

    private UnorderedInputChannel(@NotNull Buffer<M> buffer) {
      this.buffer = buffer;
    }

    public void push(M message) {
      buffer.add(message);
    }

    public void close() {
      // do nothing
    }
  }

  private static class UnorderedOutputChannel<M> implements OutputChannel<M> {

    private final Iterator<M> iterator;

    private UnorderedOutputChannel(@NotNull Iterator<M> iterator) {
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
