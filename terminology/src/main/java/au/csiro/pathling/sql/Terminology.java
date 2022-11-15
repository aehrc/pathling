package au.csiro.pathling.sql;

import au.csiro.pathling.sql.udf.TranslateCoding;
import au.csiro.pathling.sql.udf.TranslateCodingArray;
import au.csiro.pathling.sql.udf.ValidateCoding;
import au.csiro.pathling.sql.udf.ValidateCodingArray;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Terminology {

  @Nonnull
  static Column validate_coding(@Nonnull final Column coding, @Nullable final String valueSetUrl) {
    return callUDF(ValidateCoding.FUNCTION_NAME, coding, lit(valueSetUrl));
  }

  @Nonnull
  static Column validate_coding_array(@Nonnull final Column codings,
      @Nullable final String valueSetUrl) {
    return callUDF(ValidateCodingArray.FUNCTION_NAME, codings, lit(valueSetUrl));
  }

  @Nonnull
  static Column translate_coding(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return callUDF(TranslateCoding.FUNCTION_NAME, coding, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }

  @Nonnull
  static Column translate_coding_array(@Nonnull final Column codings,
      @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return callUDF(TranslateCodingArray.FUNCTION_NAME, codings, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }

}
