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

  /**
   * Creates a single primitive definition with cardinality 1.
   *
   * @param name the element name
   * @param type the FHIR type
   * @return a new DefaultPrimitiveDefinition with cardinality 1
   */
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
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return Optional.empty();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.of(type);
  }
}
