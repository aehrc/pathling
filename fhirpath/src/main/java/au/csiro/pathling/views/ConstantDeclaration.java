package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.ValidName;
import jakarta.annotation.Nonnull;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
@AllArgsConstructor
@NoArgsConstructor
public class ConstantDeclaration {

  /**
   * Name of constant (referred to in FHIRPath as {@code %[name]}).
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.name">ViewDefinition.constant.name</a>
   */
  @Nonnull
  @NotNull
  @ValidName
  String name;

  /**
   * The string that will be substituted in place of the constant reference.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant.value">ViewDefinition.constant.value</a>
   */
  @Nonnull
  @NotNull
  IBase value;

}
