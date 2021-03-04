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

import concoord.util.assertion.IfNull;
import concoord.util.assertion.IfSomeOf;
import concoord.util.assertion.Precondition;
import concoord.util.collection.CircularQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

public class Join<T> implements Iterable<T> {

  private final Iterable<T> iterable;

  public Join(@NotNull final Awaitable<T> awaitable, final int maxEvents, final long nextTimeout,
      @NotNull final TimeUnit timeUnit) {
    buildPrecondition(awaitable, timeUnit).throwException();
    this.iterable = new Iterable<T>() {
      @NotNull
      public Iterator<T> iterator() {
        return new JoinIterator<T>(awaitable, maxEvents, nextTimeout, timeUnit);
      }
    };
  }

  public Join(@NotNull final Awaitable<T> awaitable, final int maxEvents, final long nextTimeout,
      final long totalTimeout, @NotNull final TimeUnit timeUnit) {
    buildPrecondition(awaitable, timeUnit).throwException();
    this.iterable = new Iterable<T>() {
      @NotNull
      public Iterator<T> iterator() {
        return new JoinIterator<T>(awaitable, maxEvents, nextTimeout, totalTimeout, timeUnit);
      }
    };
  }

  @NotNull
  public Iterator<T> iterator() {
    return iterable.iterator();
  }

  @NotNull
  public List<T> toList() {
    final ArrayList<T> messages = new ArrayList<T>();
    for (T message : this) {
      messages.add(message);
    }
    return messages;
  }

  @NotNull
  private Precondition buildPrecondition(Awaitable<T> awaitable, TimeUnit timeUnit) {
    return new IfSomeOf(
        new IfNull(awaitable, "awaitable"),
        new IfNull(timeUnit, "timeUnit")
    );
  }

  private static class JoinIterator<T> implements Iterator<T> {

    private final Object mutex = new Object();
    private final CircularQueue<T> queue = new CircularQueue<T>();
    private final JoinAwaiter awaiter = new JoinAwaiter();
    private final Awaitable<T> awaitable;
    private final int maxEvents;
    private final TimeoutProvider timeoutProvider;
    private Throwable throwable;
    private boolean isDone;

    private JoinIterator(@NotNull Awaitable<T> awaitable, int maxEvents, long nextTimeout,
        @NotNull TimeUnit timeUnit) {
      this.awaitable = awaitable;
      this.maxEvents = maxEvents;
      this.timeoutProvider = new UnboundTimeoutProvider(timeUnit.toMillis(nextTimeout));
    }

    private JoinIterator(@NotNull Awaitable<T> awaitable, int maxEvents, long nextTimeout,
        long totalTimeout, @NotNull TimeUnit timeUnit) {
      this.awaitable = awaitable;
      this.maxEvents = maxEvents;
      this.timeoutProvider = new BoundTimeoutProvider(timeUnit.toMillis(nextTimeout),
          timeUnit.toMillis(totalTimeout));
    }

    public boolean hasNext() {
      final long startTimeMs = System.currentTimeMillis();
      final TimeoutProvider timeoutProvider = this.timeoutProvider;
      synchronized (mutex) {
        while (true) {
          long timeoutMs = timeoutProvider.getNextTimeout(startTimeMs);
          if (timeoutMs >= 0) {
            // await events
            awaitable.await(maxEvents, awaiter);
            if (!queue.isEmpty()) {
              return true;
            }
            if (isDone) {
              return false;
            }
            if (throwable != null) {
              throw new JoinException(throwable);
            }
            if (timeoutMs == 0) {
              break;
            }
            try {
              mutex.wait(timeoutMs);
            } catch (final InterruptedException e) {
              throw new JoinException(e);
            }
          } else {
            break;
          }
        }
        // timeout
        throw new JoinTimeoutException("no event received after ms: "
            + (startTimeMs - System.currentTimeMillis()));
      }
    }

    public T next() {
      synchronized (mutex) {
        return queue.remove();
      }
    }

    public void remove() {
      throw new UnsupportedOperationException("remove");
    }

    private interface TimeoutProvider {

      long getNextTimeout(long startTimeMs);
    }

    private static class UnboundTimeoutProvider implements TimeoutProvider {

      private final long nextTimeoutMs;

      private UnboundTimeoutProvider(long nextTimeoutMs) {
        this.nextTimeoutMs = nextTimeoutMs;
      }

      public long getNextTimeout(long startTimeMs) {
        return startTimeMs + nextTimeoutMs - System.currentTimeMillis();
      }
    }

    private static class BoundTimeoutProvider implements TimeoutProvider {

      private final long nextTimeoutMs;
      private final long expireTimeMs;

      private BoundTimeoutProvider(long nextTimeoutMs, long totalTimeoutMs) {
        this.nextTimeoutMs = nextTimeoutMs;
        this.expireTimeMs = System.currentTimeMillis() + totalTimeoutMs;
      }

      public long getNextTimeout(long startTimeMs) {
        long currentTimeMs = System.currentTimeMillis();
        return Math.min(startTimeMs + nextTimeoutMs - currentTimeMs, expireTimeMs - currentTimeMs);
      }
    }

    private class JoinAwaiter implements Awaiter<T> {

      public void message(T message) {
        synchronized (mutex) {
          queue.add(message);
          mutex.notifyAll();
        }
      }

      public void error(@NotNull Throwable error) {
        synchronized (mutex) {
          throwable = error;
          mutex.notifyAll();
        }
      }

      public void end() {
        synchronized (mutex) {
          isDone = true;
          mutex.notifyAll();
        }
      }
    }
  }
}
