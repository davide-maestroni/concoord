package concoord.util.assertion;

import java.util.Collection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IfContainsNull extends AbstractPrecondition {

  private final AbstractPrecondition precondition;

  public IfContainsNull(Object... objects) {
    this("objects", objects);
  }

  public IfContainsNull(Collection<Object> objects) {
    this("objects", objects);
  }

  public IfContainsNull(Iterable<Object> objects) {
    this("objects", objects);
  }

  public IfContainsNull(final String name, final Object... objects) {
    this.precondition = new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (objects != null) {
          for (Object object : objects) {
            if (object == null) {
              return buildException(name);
            }
          }
        }
        return null;
      }
    };
  }

  public IfContainsNull(final String name, final Collection<Object> objects) {
    this.precondition = new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (objects != null) {
          if (objects.contains(null)) {
            return buildException(name);
          }
        }
        return null;
      }
    };
  }

  public IfContainsNull(final String name, final Iterable<Object> objects) {
    this.precondition = new AbstractPrecondition() {
      @Nullable
      public RuntimeException getException() {
        if (objects != null) {
          for (Object object : objects) {
            if (object == null) {
              return buildException(name);
            }
          }
        }
        return null;
      }
    };
  }

  @NotNull
  private static NullPointerException buildException(String name) {
    return new NullPointerException(name + " cannot contain null elements");
  }

  @Nullable
  public RuntimeException getException() {
    return precondition.getException();
  }
}
