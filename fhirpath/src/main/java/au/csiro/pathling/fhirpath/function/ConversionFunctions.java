package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.utilities.Preconditions;
import javax.annotation.Nonnull;

/**
 * Functions for converting between different types.
 *
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#conversion">FHIRPath Specification -
 * Conversion</a>
 */
@SuppressWarnings("unused")
public class ConversionFunctions {

  /**
   * If the input collection contains a single item, this function will return a single String if:
   * <ul>
   * <li>the item in the input collection is a String</li>
   * <li>the item in the input collection is an Integer,
   * Decimal, Date, Time, DateTime, or Quantity the output will contain its String representation</li>
   * <li>the item is a Boolean, where true results in 'true' and false in 'false'.</li>
   * </ul>
   * If the item is not one of the above types, the result is false.
   *
   * @param input The input collection
   * @return A collection containing the single item as a String, or an empty collection if the
   * input collection is empty or contains more than one item.
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tostring--string">FHIRPath Specification
   * - toString</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection toString(@Nonnull final Collection input) {
    Preconditions.checkUserInput(input instanceof StringCoercible,
        "toString() can only be applied to a StringCoercible path");
    return ((StringCoercible) input).asStringPath();
  }

}
