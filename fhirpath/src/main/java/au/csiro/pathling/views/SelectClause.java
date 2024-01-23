package au.csiro.pathling.views;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Defines the content of a column within the view.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
 */
public abstract class SelectClause implements SelectionElement {

  @Nullable
  abstract String getPath();

  @Nonnull
  abstract List<Column> getColumn();

  @Nonnull
  abstract List<SelectClause> getSelect();

  @Nonnull
  abstract List<SelectClause> getUnionAll();
  
}
