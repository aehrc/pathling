package au.csiro.pathling.fhirpath;


import jakarta.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;


/**
 * Options for controlling the behaviour of FHIRPath evaluation.
 */
@Value
@Builder
public class EvalOptions {

  private static final EvalOptions DEFAULT = EvalOptions.builder().build();

  /**
   * Controls whether  to fields not defined in the FHIR schema is allowed. An empty collection will
   * be returned for any such fields when this option is set to {@code true}. Otherwise, an
   * exception will be thrown.
   */
  @Builder.Default
  boolean allowUndefinedFields = false;

  /**
   * Returns the default options.
   *
   * @return the default options
   */
  @Nonnull
  public static EvalOptions getDefaults() {
    return DEFAULT;
  }
}

