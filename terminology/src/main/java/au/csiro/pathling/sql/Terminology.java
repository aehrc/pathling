package au.csiro.pathling.sql;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.sql.udf.TranslateCoding;
import au.csiro.pathling.sql.udf.ValidateCoding;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;

public interface Terminology {

  // TODO: consider adding all column versions (could be useful e.g. to get valueSetUri from a table or template)

  // TODO: consider nullablity of the arguments
  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nullable final String valueSetUri) {
    return call_udf(ValidateCoding.FUNCTION_NAME, coding, lit(valueSetUri));
  }

  // TODO: consider the order of reverse and equivaleces
  // TODO: consider other forms of passing equivalences (i.e collection of enum types)
  // TODO: add overloaded methods for default arguments.
  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return call_udf(TranslateCoding.FUNCTION_NAME, coding, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }
}
