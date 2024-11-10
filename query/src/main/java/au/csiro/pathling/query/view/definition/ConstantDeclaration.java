package au.csiro.pathling.query.view.definition;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import org.hl7.fhir.instance.model.api.IBase;

/**
 * Constant that can be used in FHIRPath expressions.
 * <p>
 * A constant is a string that is injected into a FHIRPath expression through the use of a FHIRPath
 * external constant with the same name.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant">ViewDefinition.constant</a>
 */
@Data
public class ConstantDeclaration {

  /**
   * Name of constant (referred to in FHIRPath as {@code %[name]}).
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.name">ViewDefinition.constant.name</a>
   */
  @org.jetbrains.annotations.NotNull
  @NotNull
  String name;

  /**
   * The string that will be substituted in place of the constant reference.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.value">ViewDefinition.constant.value</a>
   */
  @org.jetbrains.annotations.NotNull
  @NotNull
  IBase value;

}
