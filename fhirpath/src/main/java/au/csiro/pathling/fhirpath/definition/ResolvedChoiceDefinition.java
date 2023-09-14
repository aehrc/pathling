package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public class ResolvedChoiceDefinition implements ElementDefinition {

  @Nonnull
  private final BaseRuntimeElementDefinition elementDefinition;

  @Nonnull
  @Getter
  private final Optional<Integer> maxCardinality;

  public ResolvedChoiceDefinition(@Nonnull final BaseRuntimeElementDefinition definition,
      @Nonnull final
      ChoiceElementDefinition parent) {
    this.elementDefinition = definition;
    this.maxCardinality = parent.getMaxCardinality();
  }

  @Nonnull
  @Override
  public String getElementName() {
    return elementDefinition.getName();
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Nonnull
  @Override
  public Optional<FHIRDefinedType> getFhirType() {
    return ElementDefinition.getFhirTypeFromElementDefinition(elementDefinition);
  }
}
