package au.csiro.pathling.fhirpath.definition.def;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import java.util.List;
import java.util.Optional;

@Value
public class DefCompositeDefinition implements ElementDefinition {

  String name;
  List<ChildDefinition> children;
  int cardinality;

  @Override
  @Nonnull
  public String getElementName() {
    return name;
  }

  @Override
  @Nonnull
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(cardinality);
  }

  @Override
  @Nonnull
  public Optional<? extends ChildDefinition> getChildElement(@Nonnull String name) {
    return Optional.empty();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.empty();
  }
}
