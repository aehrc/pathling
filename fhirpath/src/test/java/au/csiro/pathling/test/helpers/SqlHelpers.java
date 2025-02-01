package au.csiro.pathling.test.helpers;

import lombok.experimental.UtilityClass;
import scala.collection.mutable.WrappedArray;
import javax.annotation.Nonnull;

@UtilityClass
public class SqlHelpers {

  @SafeVarargs
  @Nonnull
  public static <T> WrappedArray<T> sql_array(final T... values) {
    return WrappedArray.make(values);
  }
}
