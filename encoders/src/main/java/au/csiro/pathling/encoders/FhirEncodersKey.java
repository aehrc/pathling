package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import java.util.Set;
import lombok.Value;

/**
 * Immutable key to look up a matching encoders instance by configuration.
 */
@Value
public class FhirEncodersKey {

  /**
   * The FHIR version.
   */
  @Nonnull
  FhirVersionEnum fhirVersion;

  /**
   * The maximum nesting level for recursive types.
   */
  int maxNestingLevel;

  /**
   * The set of open types to encode.
   */
  @Nonnull
  Set<String> openTypes;

  /**
   * Whether extensions are enabled.
   */
  boolean enableExtensions;

}
