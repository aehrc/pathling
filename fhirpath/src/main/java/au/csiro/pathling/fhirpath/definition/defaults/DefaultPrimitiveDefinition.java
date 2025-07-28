package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;


/**
 * The default implementation of a primitive data type allowing for explicit definition of its
 * properties.
 */
@Value(staticConstructor = "of")
public class DefaultPrimitiveDefinition implements ElementDefinition {

  @Nonnull
  public static DefaultPrimitiveDefinition single(final String name, final FHIRDefinedType type) {
    return new DefaultPrimitiveDefinition(name, type, 1);
  }

  String name;
  FHIRDefinedType type;
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
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.of(type);
  }
}
