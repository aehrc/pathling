package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * A resolver for HL7 FHIR ValueSets.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/fhirpath.html#vars">FHIR-specific environment
 * variables</a>
 */
public class Hl7ValueSetResolver implements EnvironmentVariableResolver {

  private static final String NAME_PREFIX = "vs-";

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    if (name.startsWith(NAME_PREFIX)) {
      final String valueSetName = name.substring(NAME_PREFIX.length());
      return Optional.of(
          StringCollection.fromValue("http://hl7.org/fhir/ValueSet/" + valueSetName));
    } else {
      return Optional.empty();
    }
  }

}
