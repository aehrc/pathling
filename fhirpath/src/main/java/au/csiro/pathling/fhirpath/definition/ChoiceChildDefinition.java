package au.csiro.pathling.fhirpath.definition;

import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.WordUtils;

/**
 * Represents the definition of an element that can be represented by multiple different data
 * types.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/fhirpath.html#polymorphism">Polymorphism in FHIR</a>
 */
public class ChoiceChildDefinition implements ChildDefinition {

  @Nonnull
  private final RuntimeChildChoiceDefinition childDefinition;
  
  protected ChoiceChildDefinition(@Nonnull final RuntimeChildChoiceDefinition childDefinition) {
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
  public Optional<ElementDefinition> getChildElement(@Nonnull final String name) {
    return getChildByElementName(name);
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
    final String key = ChoiceChildDefinition.getColumnName(getName(), type);
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
    return ChildDefinition.buildElement(childDefinition, name);
  }

}
