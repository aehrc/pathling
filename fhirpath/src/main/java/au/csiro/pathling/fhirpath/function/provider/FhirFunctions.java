package au.csiro.pathling.fhirpath.function.provider;

import static au.csiro.pathling.fhirpath.operator.Comparable.ComparisonOperation.EQUALS;

import au.csiro.pathling.fhirpath.annotations.SofCompatibility;
import au.csiro.pathling.fhirpath.annotations.SofCompatibility.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.operator.Comparable;
import jakarta.annotation.Nonnull;

/**
 * Additional FHIRPath functions that are defined within the FHIR specification.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIR specification - Additional
 * functions</a>
 */
public class FhirFunctions {

  private static final String URL_ELEMENT_NAME = "url";
  private static final String EXTENSION_ELEMENT_NAME = "extension";

  /**
   * Will filter the input collection for items named "extension" with the given url. This is a
   * syntactical shortcut for {@code .extension.where(url = string)}, but is simpler to write. Will
   * return an empty collection if the input collection is empty or the url is empty.
   *
   * @param input The input collection
   * @param url The URL to filter by
   * @return A collection containing only those elements for which the criteria expression evaluates
   */
  @FhirPathFunction
  @SofCompatibility(Profile.SHARABLE)
  @Nonnull
  public static Collection extension(@Nonnull final Collection input,
      @Nonnull final StringCollection url) {
    return input.traverse(EXTENSION_ELEMENT_NAME).map(extensionCollection ->
        FilteringAndProjectionFunctions.where(extensionCollection, c -> c.traverse(
                URL_ELEMENT_NAME).map(
                urlCollection -> ((Comparable) urlCollection).getComparison(EQUALS).apply(url))
            .map(col -> BooleanCollection.build(new DefaultRepresentation(col)))
            .orElse(BooleanCollection.fromValue(false)))
    ).orElse(EmptyCollection.getInstance());
  }

}
