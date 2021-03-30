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
import concoord.util.assertion.IfNull;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.NotNull;

public class JoinFuture<V> implements Future<List<V>> {

  private final Object mutex = new Object();
  private final Awaitable<V> awaitable;
  private final int maxEvents;
  private boolean isCancelled;

  public JoinFuture(@NotNull Awaitable<V> awaitable, int maxEvents) {
    new IfNull("awaitable", awaitable).throwException();
    this.awaitable = awaitable;
    this.maxEvents = maxEvents;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    synchronized (mutex) {
      if (isCancelled) {
        return false;
      }
      isCancelled = true;
      awaitable.abort();
    }
    return true;
  }

  public boolean isCancelled() {
    return false;
  }

  public boolean isDone() {
    return false;
  }

  public List<V> get() throws InterruptedException, ExecutionException {
    synchronized (mutex) {
      if (isCancelled) {
        throw new CancellationException();
      }
      try {
        // TODO: 30/03/21 cache result
        return new Join<V>(awaitable, maxEvents, Long.MAX_VALUE, TimeUnit.MILLISECONDS).toList();
      } catch (final JoinAbortException e) {
        throw new CancellationException(e.getMessage());
      } catch (final JoinTimeoutException e) {
        throw new ExecutionException(e);
      } catch (final JoinException e) {
        throw new ExecutionException(e.getCause());
      }
    }
  }

  public List<V> get(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    synchronized (mutex) {
      if (isCancelled) {
        throw new CancellationException();
      }
      try {
        // TODO: 30/03/21 cache result
        return new Join<V>(awaitable, maxEvents, Long.MAX_VALUE, unit.toMillis(timeout),
            TimeUnit.MILLISECONDS).toList();
      } catch (final JoinAbortException e) {
        throw new CancellationException(e.getMessage());
      } catch (final JoinTimeoutException e) {
        throw new TimeoutException(e.getMessage());
      } catch (final JoinException e) {
        throw new ExecutionException(e.getCause());
      }
    }
  }
}
