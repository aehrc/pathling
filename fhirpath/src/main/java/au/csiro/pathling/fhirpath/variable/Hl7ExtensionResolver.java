package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A resolver for HL7 FHIR Extensions.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#vars">FHIR-specific environment
 * variables</a>
 */
public class Hl7ExtensionResolver implements EnvironmentVariableResolver {

  private static final String NAME_PREFIX = "ext-";

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    if (name.startsWith(NAME_PREFIX)) {
      final String extensionName = name.substring(NAME_PREFIX.length());
      return Optional.of(
          StringCollection.fromValue("http://hl7.org/fhir/StructureDefinition/" + extensionName));
    } else {
      return Optional.empty();
    }
  }

}
