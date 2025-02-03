package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

@Value(staticConstructor = "of")
public class DefPrimitiveDefinition implements ElementDefinition {

  public static DefPrimitiveDefinition single(String name, FHIRDefinedType type) {
    return new DefPrimitiveDefinition(name, type, 1);
  }

  String name;
  FHIRDefinedType type;
  int cardinalit;

  @Override
  @Nonnull
  public String getElementName() {
    return name;
  }

  @Override
  @Nonnull
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(cardinalit);
  }

  @Override
  @Nonnull
  public Optional<? extends ChildDefinition> getChildElement(@Nonnull String name) {
    return Optional.empty();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.of(type);
  }
}
