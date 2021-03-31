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
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

public class FutureJoin<V> implements Future<List<V>> {

  private static final int CANCELED = -1;
  private static final int IDLE = 0;
  private static final int RUNNING = 1;
  private static final int DONE = 2;

  private final Object mutex = new Object();
  private final AtomicInteger status = new AtomicInteger(IDLE);
  private final Awaitable<V> awaitable;
  private final int maxEvents;
  private volatile List<V> result;

  public FutureJoin(@NotNull Awaitable<V> awaitable, int maxEvents) {
    new IfNull("awaitable", awaitable).throwException();
    this.awaitable = awaitable;
    this.maxEvents = maxEvents;
  }

  public boolean cancel(boolean mayInterruptIfRunning) {
    final AtomicInteger status = this.status;
    if (status.compareAndSet(IDLE, CANCELED) || status.compareAndSet(RUNNING, CANCELED)) {
      awaitable.abort();
      return true;
    }
    return false;
  }

  public boolean isCancelled() {
    return status.get() == CANCELED;
  }

  public boolean isDone() {
    return status.get() == DONE;
  }

  public List<V> get() throws InterruptedException, ExecutionException {
    final AtomicInteger status = this.status;
    if (status.compareAndSet(IDLE, RUNNING)) {
      synchronized (mutex) {
        try {
          final List<V> result =
              new Join<V>(awaitable, maxEvents, Long.MAX_VALUE, TimeUnit.MILLISECONDS).toList();
          if (status.compareAndSet(RUNNING, DONE)) {
            this.result = result;
            mutex.notifyAll();
            return result;
          } else {
            throw new CancellationException();
          }
        } catch (final JoinAbortException e) {
          throw new CancellationException(e.getMessage());
        } catch (final JoinTimeoutException e) {
          throw new ExecutionException(e);
        } catch (final JoinException e) {
          throw new ExecutionException(e.getCause());
        }
      }
    }
    synchronized (mutex) {
      while (status.get() == RUNNING) {
        mutex.wait();
      }
    }
    if (status.get() == DONE) {
      return result;
    }
    throw new CancellationException();
  }

  public List<V> get(long timeout, @NotNull TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    final AtomicInteger status = this.status;
    if (status.compareAndSet(IDLE, RUNNING)) {
      synchronized (mutex) {
        try {
          final List<V> result = new Join<V>(
              awaitable,
              maxEvents,
              Long.MAX_VALUE,
              unit.toMillis(timeout),
              TimeUnit.MILLISECONDS
          ).toList();
          if (status.compareAndSet(RUNNING, DONE)) {
            this.result = result;
            mutex.notifyAll();
            return result;
          } else {
            throw new CancellationException();
          }
        } catch (final JoinAbortException e) {
          throw new CancellationException(e.getMessage());
        } catch (final JoinTimeoutException e) {
          throw new TimeoutException(e.getMessage());
        } catch (final JoinException e) {
          throw new ExecutionException(e.getCause());
        }
      }
    }
    synchronized (mutex) {
      while (status.get() == RUNNING) {
        mutex.wait();
      }
    }
    if (status.get() == DONE) {
      return result;
    }
    throw new CancellationException();
  }
}
