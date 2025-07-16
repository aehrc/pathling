package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.UniqueColumnNames;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * View definitions are the heart of this proposal. In a nutshell, view is tabular projection of a
 * FHIR resource, where the columns and criteria are defined by FHIRPath expressions. This is
 * defined in a simple JSON document for ease of use and to iterate quickly, but future iterations
 * may shift to defining views as true FHIR resources.
 *
 * @author John Grimes
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html">ViewDefinition</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@UniqueColumnNames
public class FhirView {

  @Nonnull
  public static FhirViewBuilder builder() {
    return new FhirViewBuilder();
  }

  /**
   * Creates a builder with the resource already set.
   *
   * @param resource the resource name to set
   * @return a builder with the resource set
   */
  @Nonnull
  public static FhirViewBuilder ofResource(@Nonnull final String resource) {
    return FhirView.builder().resource(resource);
  }

  /**
   * Creates a builder with the resource already set.
   *
   * @param resource the {@link ResourceType} to set
   * @return a builder with the resource set
   */
  @Nonnull
  public static FhirViewBuilder ofResource(@Nonnull final ResourceType resource) {
    return ofResource(resource.toCode());
  }

  /**
   * Creates a new {@link SelectClause} with the given columns.
   *
   * @param columns the columns to include
   * @return a new {@link SelectClause} instance
   */
  @Nonnull
  public static SelectClause columns(@Nonnull final Column... columns) {
    return SelectClause.builder().column(columns).build();
  }

  @Nonnull
  public static Column column(@Nonnull final String name, @Nonnull final String path) {
    return new Column(name, path);
  }

  @Nonnull
  public static SelectClause select(@Nonnull final Column... columns) {
    return SelectClause.builder().column(columns).build();
  }

  @Nonnull
  public static SelectClause select(@Nonnull final SelectClause... selects) {
    return SelectClause.builder().select(selects).build();
  }

  @Nonnull
  public static SelectClause forEach(@Nonnull final String forEach,
      @Nonnull final Column... columns) {
    return SelectClause.builder()
        .forEach(forEach)
        .column(columns).build();
  }

  @Nonnull
  public static SelectClause forEach(@Nonnull final String forEach,
      @Nonnull final SelectClause... selects) {
    return SelectClause.builder()
        .forEach(forEach)
        .select(selects).build();
  }

  /**
   * Creates a new 'forEachOrNull' {@link SelectClause} with the given columns.
   *
   * @param path the path to set in the 'forEachOrNull' clause
   * @param columns the columns to include
   * @return a new {@link SelectClause} instance
   */
  @Nonnull
  public static SelectClause forEachOrNull(@Nonnull final String path,
      @Nonnull final Column... columns) {
    return SelectClause.builder()
        .forEachOrNull(path)
        .column(columns).build();
  }

  @Nonnull
  public static SelectClause forEachOrNull(@Nonnull final String path,
      @Nonnull final SelectClause... selects) {
    return SelectClause.builder()
        .forEachOrNull(path)
        .select(selects).build();
  }

  @Nonnull
  public static SelectClause unionAll(@Nonnull final SelectClause... selects) {
    return SelectClause.builder().unionAll(selects).build();
  }

  /**
   * The FHIR resource that the view is based upon, e.g. 'Patient' or 'Observation'.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.resource">ViewDefinition.resource</a>
   */
  @Nonnull
  @NotNull
  String resource;

  /**
   * Constant that can be used in FHIRPath expressions.
   * <p>
   * A constant is a string that is injected into a FHIRPath expression through the use of a
   * FHIRPath external constant with the same name.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant">ViewDefinition.constant</a>
   */
  @Nonnull
  @NotNull
  @Valid
  List<@Valid ConstantDeclaration> constant = Collections.emptyList();

  /**
   * Defines the content of a column within the view.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
   */
  @Nonnull
  @NotNull
  @NotEmpty
  @Valid
  List<@Valid SelectClause> select = Collections.emptyList();

  /**
   * FHIRPath expression defining a filter condition.
   * <p>
   * A FHIRPath expression that defines a filter that must evaluate to true for a resource to be
   * included in the output. The input context is the collection of resources of the type specified
   * in the resource element. Constants defined in {@link #constant} can be referenced as
   * {@code %[name]}. The result of the expression must be of type Boolean.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.where">ViewDefinition.where</a>
   */
  @Nullable
  @Valid
  List<@Valid WhereClause> where = null;

  /**
   * Gets the names of all columns in the view including those in nested selects.
   *
   * @return a stream of all columns in the view
   */
  @Nonnull
  public Stream<Column> getAllColumns() {
    return select.stream()
        .flatMap(SelectClause::getAllColumns);
  }

}
