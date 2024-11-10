package au.csiro.pathling.query.view.definition;

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Defines the content of a column within the view.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
 */
public abstract class SelectClause implements SelectionElement {

  @NotNull
  abstract List<Column> getColumn();

  @NotNull
  abstract List<SelectClause> getSelect();

  @NotNull
  abstract List<SelectClause> getUnionAll();

}
