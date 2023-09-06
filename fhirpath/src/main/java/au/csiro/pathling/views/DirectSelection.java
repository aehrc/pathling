package au.csiro.pathling.views;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Describes the selection of a column in the output.
 *
 * @author John Grimes
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DirectSelection extends SelectClause {

  /**
   * Name of the column produced in the output, must be in a database-friendly format.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.name">ViewDefinition.select.name</a>
   */
  @NotNull
  @Size(max = 255)
  @Pattern(regexp = "^[^_][A-Za-z][A-Za-z0-9_]+$")
  String alias;

  /**
   * A FHIRPath expression that evaluates to the value that will be output in the column for each
   * resource. The input context is the collection of resources of the type specified in the
   * resource element. Constants defined in {@link FhirView#constants} can be referenced as
   * {@code %[name]}.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.path">ViewDefinition.select.path</a>
   */
  @NotNull
  String path;

  /**
   * A human-readable description of the column.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.description">ViewDefinition.select.description</a>
   */
  @Nullable
  String description;

}
