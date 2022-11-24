package au.csiro.pathling.sql;

import static org.apache.spark.sql.functions.call_udf;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.sql.udf.SubsumesUdf;
import au.csiro.pathling.sql.udf.TranslateUdf;
import au.csiro.pathling.sql.udf.MemberOfUdf;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;

/**
 * JAVA API for terminology UDFs
 */
public interface Terminology {

  /**
   * Takes a Coding or an array of Codings column as its input. Returns the column which, contains a
   * Boolean value, indicating whether any of the input Codings is the member of the specified FHIR
   * ValueSet.
   *
   * @param coding a Column containing the struct representation of a Coding or an array of such
   * structs.
   * @param valueSetUrl the identifier for a FHIR ValueSet.
   * @return the Column containing the result of the operation
   */
  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nonnull final String valueSetUrl) {
    return member_of(coding, lit(valueSetUrl));
  }

  /**
   * Takes a Coding or an array of Codings column as its input. Returns the column which, contains a
   * Boolean value, indicating whether any of the input Codings is the member of the specified FHIR
   * ValueSet.
   *
   * @param coding a Column containing the struct representation of a Coding or an array of such
   * structs.
   * @param valueSetUrl the column with the identifier for a FHIR ValueSet.
   * @return the Column containing the result of the operation
   */
  @Nonnull
  static Column member_of(@Nonnull final Column coding, @Nonnull final Column valueSetUrl) {
    return call_udf(MemberOfUdf.FUNCTION_NAME, coding, valueSetUrl);
  }


  // TODO: consider the order of reverse and equivaleces
  // TODO: consider other forms of passing equivalences (i.e collection of enum types)
  // TODO: add overloaded methods for default arguments.
  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences, @Nullable final String target) {
    return call_udf(TranslateUdf.FUNCTION_NAME, coding, lit(conceptMapUri), lit(reverse),
        lit(equivalences));
  }
  
  @Nonnull
  static Column translate(@Nonnull final Column coding, @Nonnull final String conceptMapUri,
      boolean reverse, @Nullable final String equivalences) {
    return translate(coding, conceptMapUri, reverse, equivalences, null);
  }


  /**
   * Takes two Coding or array of Codings columns as its input. Returns the Column, which contains a
   * Boolean value, indicating whether the left Coding subsumes the right Coding.
   *
   * @param codingA a Column containing a struct representation of a Coding or an array of Codings.
   * @param codingB a Column containing a struct representation of a Coding or an array of *
   * Codings.
   * @return the Column containing the result of the operation (boolean)
   */
  @Nonnull
  static Column subsumes(@Nonnull final Column codingA, @Nonnull final Column codingB) {
    return call_udf(SubsumesUdf.FUNCTION_NAME, codingA, codingB, lit(false));
  }

  /**
   * Takes two Coding or array of Codings columns as its input. Returns the Column, which contains a
   * Boolean value, indicating whether the left Coding is subsumed by the right Coding.
   *
   * @param codingA a Column containing a struct representation of a Coding or an array of Codings.
   * @param codingB a Column containing a struct representation of a Coding or an array of *
   * Codings.
   * @return the Column containing the result of the operation (boolean)
   */
  @Nonnull
  static Column subsumed_by(@Nonnull final Column codingA, @Nonnull final Column codingB) {
    return call_udf(SubsumesUdf.FUNCTION_NAME, codingA, codingB, lit(true));
  }
}
