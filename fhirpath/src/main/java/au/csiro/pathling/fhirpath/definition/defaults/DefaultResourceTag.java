package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.ResourceTag;
import jakarta.annotation.Nonnull;
import lombok.Value;

/**
 * The default implementation of a resource tag.
 */
@Value(staticConstructor = "of")
public class DefaultResourceTag implements ResourceTag {

  @Nonnull
  String code;

  @Override
  @Nonnull
  public String toCode() {
    return code;
  }

}
