package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import au.csiro.pathling.fhirpath.function.annotation.RequiredParameter;

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
  public static Collection extension(final Collection input,
      @RequiredParameter final StringCollection url) {
    throw new RuntimeException("Not implemented");
  }

}
