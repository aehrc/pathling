package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.ResourceTag;
import jakarta.annotation.Nonnull;
import lombok.Value;

@Value(staticConstructor = "of")
public class DefResourceTag implements ResourceTag {

  @Nonnull
  String code;

  @Override
  @Nonnull
  public String toCode() {
    return code;
  }

}
