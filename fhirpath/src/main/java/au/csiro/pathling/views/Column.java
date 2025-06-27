package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.ValidName;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Describes the selection of a column in the output.
 *
 * @author John Grimes
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = false)
public class Column implements SelectionElement {

  @SuppressWarnings("unused")
  public static class ColumnBuilder {
    // for javadocs
  }

  /**
   * Static factory method to create a new non collection {@link Column} instance with required
   * fields.
   *
   * @param name the name of the column, must be in a database-friendly format
   * @param path the FHIRPath expression that evaluates to the value for the column
   * @return a new {@link Column} instance
   */
  public static Column single(@NotNull final String name,
      @NotNull final String path) {
    return builder()
        .name(name)
        .path(path)
        .build();
  }

  /**
   * Name of the column produced in the output, must be in a database-friendly format.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.name">ViewDefinition.select.name</a>
   */
  @NotNull
  @Size(max = 255)
  @ValidName
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

  /**
   * Additional metadata describing the column. Tags can be used to provide database-specific type
   * information or other metadata about the column.
   * 
   * @see <a href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#type-hinting-with-tags">Type Hinting with Tags</a>
   */
  @Nullable
  @Valid
  @Builder.Default
  List<@Valid ColumnTag> tag = Collections.emptyList();


  /**
   * Checks if this column is compatible with another column for union operations. Columns are
   * compatible if they have the same type and collection indicator.
   *
   * @param other the other column to compare with
   * @return true if the columns are compatible, false otherwise
   */
  public boolean isCompatibleWith(@Nullable final Column other) {
    if (other == null) {
      return false;
    }

    // Check collection indicator
    if (this.isCollection() != other.isCollection()) {
      return false;
    }

    // Check type compatibility
    if (this.getType() == null && other.getType() == null) {
      return true; // Both have null type, considered compatible
    }

    if (this.getType() == null || other.getType() == null) {
      return false; // One has type, the other doesn't
    }

    // Check if types match
    if (!this.getType().equals(other.getType())) {
      return false;
    }

    // Tags don't affect compatibility for union operations
    // They are metadata that don't change the underlying data type
    
    return true;
  }

}
