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
package concoord.util.assertion;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IfSomeOf extends AbstractPrecondition {

  private final Collection<? extends Precondition> preconditions;

  public IfSomeOf(@NotNull Precondition... preconditions) {
    this(Arrays.asList(preconditions));
  }

  public IfSomeOf(@NotNull Collection<? extends Precondition> preconditions) {
    this.preconditions = preconditions;
  }

  @Nullable
  public RuntimeException getException() {
    List<RuntimeException> exceptions = getExceptions();
    if (!exceptions.isEmpty()) {
      return buildException(exceptions);
    }
    return null;
  }

  @NotNull
  private List<RuntimeException> getExceptions() {
    final ArrayList<RuntimeException> exceptions = new ArrayList<RuntimeException>();
    for (Precondition precondition : preconditions) {
      RuntimeException exception = precondition.getException();
      if (exception != null) {
        exceptions.add(exception);
      }
    }
    return exceptions;
  }

  @NotNull
  private PreconditionFailedException buildException(@NotNull List<RuntimeException> exceptions) {
    return new PreconditionFailedException("preconditions failed", exceptions);
  }
}
