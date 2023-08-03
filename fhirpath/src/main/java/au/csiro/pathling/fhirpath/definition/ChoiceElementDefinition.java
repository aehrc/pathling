package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.commons.lang.WordUtils;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public class ChoiceElementDefinition implements ElementDefinition {

  @Nonnull
  private final RuntimeChildChoiceDefinition childDefinition;

  private final Map<String, BaseRuntimeElementDefinition<?>> elementNameToDefinition;

  protected ChoiceElementDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition) {
    this.childDefinition = childDefinition;
    elementNameToDefinition = new HashMap<>();
    childDefinition.getValidChildNames()
        .forEach(name -> elementNameToDefinition.put(name, childDefinition.getChildByName(name)));
  }

  @Nonnull
  @Override
  public String getElementName() {
    return childDefinition.getElementName();
  }

  @Nonnull
  @Override
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Override
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(childDefinition.getMax());
  }

  @Nonnull
  @Override
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.empty();
  }

  @Nonnull
  public Optional<ElementDefinition> getChildByType(@Nonnull final String type) {
    final String key = ChoiceElementDefinition.getColumnName(getElementName(), type);
    return Optional.ofNullable(elementNameToDefinition.get(key))
        .map(def -> new ResolvedChoiceDefinition(def, this));
  }

  @Nonnull
  public static String getColumnName(@Nonnull final String elementName,
      @Nonnull final String type) {
    return elementName + WordUtils.capitalize(type);
  }

}
