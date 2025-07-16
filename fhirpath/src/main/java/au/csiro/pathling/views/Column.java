package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.UniqueTags;
import au.csiro.pathling.views.validation.ValidName;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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
   * Static factory method to create a new non collection {@link ColumnBuilder} instance with
   * required fields.
   *
   * @param name the name of the column (must be a valid identifier)
   * @param path the FHIRPath expression that evaluates to the value for the column
   * @return a new {@link ColumnBuilder} instance
   */

  @Nonnull
  public static ColumnBuilder singleBuilder(@Nonnull final String name,
      @Nonnull final String path) {
    return builder()
        .name(name)
        .path(path);
  }

  /**
   * Static factory method to create a new collection {@link ColumnBuilder} instance with required
   * fields.
   *
   * @param name the name of the column (must be a valid identifier)
   * @param path the FHIRPath expression that evaluates to the value for the column
   * @return a new {@link ColumnBuilder} instance
   */
  @Nonnull
  public static ColumnBuilder collectionBuilder(@Nonnull final String name,
      @Nonnull final String path) {
    return builder()
        .name(name)
        .path(path)
        .collection(true);
  }


  /**
   * Static factory method to create a new non collection {@link Column} instance with required
   * fields.
   *
   * @param name the name of the column (must be a valid identifier)
   * @param path the FHIRPath expression that evaluates to the value for the column
   * @return a new {@link Column} instance
   */
  @Nonnull
  public static Column single(@Nonnull final String name,
      @Nonnull final String path) {
    return singleBuilder(name, path)
        .collection(false)
        .build();
  }

  /**
   * Static factory method to create a new collection {@link Column} instance with required fields.
   *
   * @param name the name of the column (must be a valid identifier)
   * @param path the FHIRPath expression that evaluates to the value for the column
   * @return a new {@link Column} instance
   */
  @Nonnull
  public static Column collection(@Nonnull final String name,
      @Nonnull final String path) {
    return collectionBuilder(name, path)
        .build();
  }

  /**
   * Name of the column produced in the output, must be in a database-friendly format.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.name">ViewDefinition.select.name</a>
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
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.path">ViewDefinition.select.path</a>
   */
  @NotNull
  String path;

  /**
   * A human-readable description of the column.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.description">ViewDefinition.select.description</a>
   */
  @Nullable
  String description;

  /**
   * Indicates whether the column may have multiple values. Defaults to false if unset.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.collection">ViewDefinition.select.collection</a>
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
   * non-primitive type is returned, but this field is unset.
   */
  @Nullable
  String type;

  /**
   * Additional metadata describing the column. Tags can be used to provide database-specific type
   * information or other metadata about the column.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#type-hinting-with-tags">Type
   * Hinting with Tags</a>
   */
  @NotNull
  @Valid
  @UniqueTags(ColumnTag.ANSI_TYPE_TAG)
  @Builder.Default
  List<@Valid ColumnTag> tag = Collections.emptyList();


  /**
   * Checks if this column is compatible with another column for union operations. Columns are
   * compatible if they have the same name, type and collection indicator.
   *
   * @param other the other column to compare with
   * @return true if the columns are compatible, false otherwise
   */
  public boolean isCompatibleWith(@Nullable final Column other) {
    if (other == null) {
      return false;
    }

    // Check name equality
    if (!this.getName().equals(other.getName())) {
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
    return this.getType().equals(other.getType());

    // Tags don't affect compatibility for union operations
    // They are metadata that don't change the underlying data type
  }

  /**
   * Returns a list of values for all tags with the specified name.
   *
   * @param name the name of the tags to find
   * @return a list of values for tags with the specified name, may be empty if no matching tags
   * exist
   */
  @Nonnull
  public List<String> getTagValues(@Nonnull final String name) {
    return tag.stream()
        .filter(t -> name.equals(t.getName()))
        .map(ColumnTag::getValue)
        .toList();
  }

  /**
   * Returns a single value for a tag with the specified name.
   *
   * @param name the name of the tag to find
   * @return an Optional containing the value of the tag, or empty if no matching tag exists
   * @throws IllegalStateException if more than one tag with the specified name exists
   */
  @Nonnull
  public Optional<String> getTagValue(@Nonnull final String name) {
    List<String> values = getTagValues(name);
    if (values.isEmpty()) {
      return Optional.empty();
    } else if (values.size() == 1) {
      return Optional.of(values.get(0));
    } else {
      throw new IllegalStateException("Multiple values found for tag '" + name + "': " + values);
    }
  }

}
