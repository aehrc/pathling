package au.csiro.pathling.fhirpath.definition.fhir;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ChoiceDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.commons.lang.WordUtils;

/**
 * Represents the definition of an element that can be represented by multiple different data
 * types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#polymorphism">Polymorphism in FHIR</a>
 */
class FhirChoiceDefinition implements ChoiceDefinition {

  @Nonnull
  private final RuntimeChildChoiceDefinition childDefinition;

  protected FhirChoiceDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition) {
    this.childDefinition = childDefinition;
  }

  /**
   * Returns the column name for a given type.
   *
   * @param elementName the name of the parent element
   * @param type the type of the child element
   * @return the column name
   */
  @Nonnull
  public static String getColumnName(@Nonnull final String elementName,
      @Nonnull final String type) {
    return elementName + WordUtils.capitalize(type);
  }

  @Nonnull
  @Override
  public String getName() {
    return childDefinition.getElementName();
  }

  @Nonnull
  @Override
  public Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return getChildByElementName(name).map(e -> e);
  }

  @Nonnull
  @Override
  public Optional<Integer> getMaxCardinality() {
    return Optional.of(childDefinition.getMax());
  }


  /**
   * Returns the child element definition for the given type, if it exists.
   *
   * @param type the type of the child element
   * @return the child element definition, if it exists
   */
  @Nonnull
  public Optional<ElementDefinition> getChildByType(@Nonnull final String type) {
    final String key = FhirChoiceDefinition.getColumnName(getName(), type);
    return getChildByElementName(key);
  }

  /**
   * Returns the child element definition for the given element name, if it exists.
   *
   * @param name the name of the child element
   * @return the child element definition, if it exists
   */
  @Nonnull
  private Optional<ElementDefinition> getChildByElementName(final String name) {
    return FhirDefinitionContext.buildElement(childDefinition, name);
  }

}
