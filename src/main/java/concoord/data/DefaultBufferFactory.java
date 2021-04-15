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
package concoord.data;

import org.jetbrains.annotations.NotNull;

public class DefaultBufferFactory<T> implements BufferFactory<T> {

  private final BufferFactory<T> factory;

  public DefaultBufferFactory() {
    this.factory = new BufferFactoryDefault<T>();
  }

  public DefaultBufferFactory(int initialCapacity) {
    this.factory = new BufferFactoryCapacity<T>(initialCapacity);
  }

  @NotNull
  public Buffer<T> create() throws Exception {
    return factory.create();
  }

  private static class BufferFactoryDefault<T> implements BufferFactory<T> {

    @NotNull
    public Buffer<T> create() {
      return new Buffered<T>();
    }
  }

  private static class BufferFactoryCapacity<T> implements BufferFactory<T> {

    private final int initialCapacity;

    public BufferFactoryCapacity(int initialCapacity) {
      this.initialCapacity = initialCapacity;
    }

    @NotNull
    public Buffer<T> create() {
      return new Buffered<T>(initialCapacity);
    }
  }
}
