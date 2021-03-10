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

public class IfSomeOf extends AbstractFailureCondition {

  private final Collection<? extends FailureCondition> conditions;

  public IfSomeOf(@NotNull FailureCondition... conditions) {
    this(Arrays.asList(conditions));
  }

  public IfSomeOf(@NotNull Collection<? extends FailureCondition> conditions) {
    this.conditions = conditions;
  }

  @Nullable
  public RuntimeException getException() {
    final List<RuntimeException> exceptions = getExceptions();
    if (!exceptions.isEmpty()) {
      return buildException(exceptions);
    }
    return null;
  }

  @NotNull
  private List<RuntimeException> getExceptions() {
    final ArrayList<RuntimeException> exceptions = new ArrayList<RuntimeException>();
    for (final FailureCondition condition : conditions) {
      final RuntimeException exception = condition.getException();
      if (exception != null) {
        exceptions.add(exception);
      }
    }
    return exceptions;
  }

  @NotNull
  private FailureConditionException buildException(@NotNull List<RuntimeException> exceptions) {
    return new FailureConditionException("some conditions failed", exceptions);
  }
}
