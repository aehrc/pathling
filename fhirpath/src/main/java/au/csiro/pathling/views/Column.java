package au.csiro.pathling.views;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Describes the selection of a column in the output.
 *
 * @author John Grimes
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class Column implements SelectionElement {

  /**
   * Name of the column produced in the output, must be in a database-friendly format.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.name">ViewDefinition.select.name</a>
   */
  @NotNull
  @Size(max = 255)
  @Pattern(regexp = "^[^_][A-Za-z][A-Za-z0-9_]+$")
  String name;

  /**
   * A FHIRPath expression that evaluates to the value that will be output in the column for each
   * resource. The input context is the collection of resources of the type specified in the
   * resource element. Constants defined in {@link FhirView#constant} can be referenced as
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

  /**
   * Indicates whether the column may have multiple values. Defaults to false if unset.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.collection">ViewDefinition.select.collection</a>
   */
  boolean collection;

  /**
   * A FHIR {@code StructureDefinition} URI for the column's type. Relative URIs are implicitly
   * given the 'http://hl7.org/fhir/StructureDefinition/' prefix. The URI may also use FHIR element
   * ID notation to indicate a backbone element within a structure. For instance,
   * {@code Observation.referenceRange} may be specified to indicate the returned type is that
   * backbone element.
   * <p>
   * This field must be provided if a ViewDefinition returns a non-primitive type. Implementations
   * should report an error if the returned type does not match the type set here, or if a
   * non-primitive type is returned but this field is unset.
   */
  @Nullable
  String type;

}
