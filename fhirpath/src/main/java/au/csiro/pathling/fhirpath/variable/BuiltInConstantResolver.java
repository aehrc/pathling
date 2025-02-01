package au.csiro.pathling.fhirpath.variable;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

/**
 * A resolver for built-in string constants.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhirpath/#environment-variables">FHIRPath environment variables</a>
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#variables">FHIR-specific environment
 * variables</a>
 */
public class BuiltInConstantResolver implements EnvironmentVariableResolver {

  private final Map<String, String> builtInVariables = Map.of(
      "ucum", "http://unitsofmeasure.org",
      "loinc", "http://loinc.org",
      "sct", "http://snomed.info/sct"
  );

  @Override
  public Optional<Collection> get(@Nonnull final String name) {
    return Optional.ofNullable(builtInVariables.get(name)).map(StringCollection::fromValue);
  }

}
