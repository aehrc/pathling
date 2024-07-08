package au.csiro.pathling.fhirpath.function.provider;

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Contains functions for manipulating strings.
 *
 * @see <a
 * href="https://build.fhir.org/ig/HL7/FHIRPath/#wherecriteria--expression--collection">FHIRPath
 * Specification - Additional String Functions</a>
 */
public class StringFunctions {

  private static final String JOIN_DEFAULT_SEPARATOR = "";

  /**
   * The join function takes a collection of strings and joins them into a single string, optionally
   * using the given separator.
   * <p>
   * If the input is empty, the result is empty.
   * <p>
   * If no separator is specified, the strings are directly concatenated.
   *
   * @param input The input collection
   * @param separator The separator to use
   * @return A {@link StringCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#joinseparator-string--string">FHIRPath
   * Specification - join</a>
   */
  @FhirPathFunction
  @Nonnull
  public static StringCollection join(@Nonnull final StringCollection input,
      @Nullable final StringCollection separator) {
    return StringCollection.build(input.getColumn().join(
        nonNull(separator)
        ? separator.asSingular().getColumn()
        : DefaultRepresentation.literal(JOIN_DEFAULT_SEPARATOR)
    ));
  }

}
