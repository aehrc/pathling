package au.csiro.pathling.sql;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.sql.udf.TranslateCoding;
import au.csiro.pathling.sql.udf.TranslateCodingArray;
import au.csiro.pathling.sql.udf.ValidateCoding;
import au.csiro.pathling.sql.udf.ValidateCodingArray;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;

public interface Terminology {

  // TODO: consider unification of array function with runtime recognition of column type
  // TODO: consider nullablity of the arguments

  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nullable final String valueSetUri) {
    return call_udf(ValidateCoding.FUNCTION_NAME, coding, lit(valueSetUri));
  }

  @Nonnull
  static Column member_of_array(@Nonnull final Column codings,
      @Nullable final String valueSetUri) {
    return call_udf(ValidateCodingArray.FUNCTION_NAME, codings, lit(valueSetUri));
  }

  // TODO: consider the order of reverse and equivaleces
  // TODO: consider other forms of passing equivalences (i.e collection of enum types)

  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return call_udf(TranslateCoding.FUNCTION_NAME, coding, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }

  @Nonnull
  static Column translate_array(@Nonnull final Column codings,
      @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return call_udf(TranslateCodingArray.FUNCTION_NAME, codings, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }

}
