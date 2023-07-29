package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Data;

/**
 * View definitions are the heart of this proposal. In a nutshell, view is tabular projection of a
 * FHIR resource, where the columns and criteria are defined by FHIRPath expressions. This is
 * defined in a simple JSON document for ease of use and to iterate quickly, but future iterations
 * may shift to defining views as true FHIR resources.
 *
 * @author John Grimes
 */
@Data
public class FhirView {

  /**
   * Name of the FHIR view to be created. View runners can use this in whatever way is appropriate
   * for their use case, such as a database view or table name.
   * <p>
   * The name is limited to letters, numbers, or underscores and cannot start with an underscore --
   * i.e. with a regular expression of {@code }^[^_][A-Za-z0-9_]+$}. This makes it usable as table
   * names in a wide variety of databases.
   */
  @NotNull
  String name;

  /**
   * An optional human-readable description of the view.
   */
  @SerializedName("desc")
  @Nullable
  String description;

  /**
   * The FHIR resource the view is based on, e.g. 'Patient' or 'Observation'.
   */
  @NotNull
  String resource;

  /**
   * An optional list of constants that can be used in any FHIRPath expression in the view
   * definition.  These are effectively strings or numbers that can be injected into FHIRPath
   * expressions below by having {@code %constantName`} in  the expression.
   */
  @Nullable
  List<ConstantDeclaration> constants;

  /**
   * The select stanza defines the actual content of the view itself. This stanza is a list where
   * each item in is one of:
   * <ul>
   * <li>a structure with the column name, expression, and optional description,</li>
   * <li>a 'from' structure indicating a relative path to pull fields from in a nested select, or</li>
   * <li>a 'forEach' structure, unrolling the specified path and creating a new row for each item.</li>
   * </ul>
   * See the comments below for details on the semantics.
   */
  @NotNull
  @Size(min = 1)
  List<SelectClause> select;

  /**
   * 'where' filters are FHIRPath expressions joined with an implicit "and". This enables users to
   * select a subset of rows that match a specific need. For example, a user may be interested only
   * in a subset of observations based on code value and can filter them here.
   */
  @Nullable
  List<WhereClause> where;

}
