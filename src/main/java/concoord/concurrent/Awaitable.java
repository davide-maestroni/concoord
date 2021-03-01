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
package concoord.concurrent;

import org.jetbrains.annotations.NotNull;

public interface Awaitable<T> {

  void await(int maxEvents);

  void await(int maxEvents, @NotNull Awaiter<? super T> awaiter);

  void await(int maxEvents, @NotNull UnaryAwaiter<? super T> messageAwaiter,
      @NotNull UnaryAwaiter<? super Throwable> errorAwaiter, @NotNull NullaryAwaiter endAwaiter);

  void abort();

  // TODO: 25/02/21 cancel(awaiter)?
  // TODO: 01/03/21 abort created awaitables?
}
