package au.csiro.pathling.views;

import static java.util.Objects.nonNull;

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
import lombok.Builder;
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
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html">ViewDefinition</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@UniqueColumnNames
public class FhirView {

  /**
   * The customized Lombok builder for {@link FhirView}. This builder provides convenience methods.
   */
  @SuppressWarnings("unused")
  public static class FhirViewBuilder {

    /**
     * Convenience method to create select clauses from a variable-length argument list of
     * {@link SelectClause}.
     */
    public FhirViewBuilder select(@Nonnull final SelectClause... selects) {
      return select(List.of(selects));
    }

    /**
     * Convenience method to create where clauses from a variable-length argument list of
     * {@link WhereClause}.
     */
    public FhirViewBuilder where(@Nonnull final WhereClause... wheres) {
      return where(List.of(wheres));
    }

    /**
     * Convenience method to create constants from a variable-length argument list of
     * {@link ConstantDeclaration}.
     */
    public FhirViewBuilder constant(@Nonnull final ConstantDeclaration... constants) {
      return constant(List.of(constants));
    }
  }

  /**
   * Creates a builder with the resource already set.
   *
   * @param resource the resource to set
   * @return a builder with the resource set
   */
  @Nonnull
  public static FhirViewBuilder withResource(@Nonnull final String resource) {
    return builder().resource(resource);
  }

  /**
   * Creates a builder with the resource already set.
   *
   * @param resource the {@link ResourceType} to set
   * @return a builder with the resource set
   */
  @Nonnull
  public static FhirViewBuilder withResource(@Nonnull final ResourceType resource) {
    return builder().resource(resource.toCode());
  }

  /**
   * The FHIR resource that the view is based upon, e.g. 'Patient' or 'Observation'.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.resource">ViewDefinition.resource</a>
   */
  @NotNull
  String resource;

  /**
   * Constant that can be used in FHIRPath expressions.
   * <p>
   * A constant is a string that is injected into a FHIRPath expression through the use of a
   * FHIRPath external constant with the same name.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.constant">ViewDefinition.constant</a>
   */
  @NotNull
  @Valid
  @Builder.Default
  List<@Valid ConstantDeclaration> constant = Collections.emptyList();

  /**
   * Defines the content of a column within the view.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
   */
  @NotNull
  @NotEmpty
  @Valid
  @Builder.Default
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
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.where">ViewDefinition.where</a>
   */
  @Nullable
  @Valid
  @Builder.Default
  List<@Valid WhereClause> where = null;

  /**
   * Gets the names of all columns in the view including those in nested selects.
   *
   * @return a stream of all columns in the view
   */
  @Nonnull
  public Stream<Column> getAllColumns() {
    return nonNull(select)
           ? select.stream()
               .flatMap(SelectClause::getAllColumns)
           : Stream.empty();
  }
}
