package au.csiro.pathling.fhirpath.definition.defaults;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The default implementation of a composite data type allowing for explicit definition of its
 * children and other properties.
 */
@Value(staticConstructor = "of")
public class DefaultCompositeDefinition implements ElementDefinition {

  String name;
  List<ChildDefinition> children;
  int cardinality;
  FHIRDefinedType type;

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
    return children.stream()
        .filter(child -> child.getName().equals(name))
        .findFirst();
  }

  @Override
  @Nonnull
  public Optional<FHIRDefinedType> getFhirType() {
    return Optional.of(type);
  }

  /**
   * Creates a backbone element definition.
   *
   * @param name the element name
   * @param children the child definitions
   * @param cardinality the cardinality
   * @return a new DefaultCompositeDefinition for a backbone element
   */
  @Nonnull
  public static DefaultCompositeDefinition backbone(@Nonnull final String name,
      @Nonnull final List<ChildDefinition> children, final int cardinality) {
    return new DefaultCompositeDefinition(name, children, cardinality, FHIRDefinedType.BACKBONEELEMENT);
  }

}
