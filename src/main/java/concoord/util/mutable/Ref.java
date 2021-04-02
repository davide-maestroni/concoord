package concoord.util.mutable;

public class Ref<T> {

  private T value;

  public Ref() {
  }

  public Ref(T value) {
    this.value = value;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Ref)) {
      return false;
    }

    Ref<?> other = (Ref<?>) o;
    return value != null ? value.equals(other.getValue()) : other.getValue() == null;
  }

  @Override
  public int hashCode() {
    return value != null ? value.hashCode() : 0;
  }

  @Override
  public String toString() {
    return String.valueOf(value);
  }
}
