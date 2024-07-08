package au.csiro.pathling.views;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;
import lombok.Data;

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
public class FhirView {

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
  @Nullable
  List<ConstantDeclaration> constant;

  /**
   * Defines the content of a column within the view.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
   */
  @NotNull
  @Size(min = 1)
  List<SelectClause> select;

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
  List<WhereClause> where;

}
