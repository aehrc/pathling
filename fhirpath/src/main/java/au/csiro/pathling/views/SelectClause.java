package au.csiro.pathling.views;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Defines the content of a column within the view.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select">ViewDefinition.select</a>
 */
public abstract class SelectClause implements SelectionElement {

  @Nonnull
  public abstract List<Column> getColumn();

  @Nonnull
  public abstract List<SelectClause> getSelect();

  @Nonnull
  public abstract List<SelectClause> getUnionAll();

  /**
   * Returns a stream of all columns defined in this select clause, including those in nested
   * selects and the unionAll.
   *
   * @return a stream of all columns
   */
  @Nonnull
  public Stream<Column> getAllColumns() {
    return Stream.of(
        getColumn().stream(),
        getSelect().stream().flatMap(SelectClause::getAllColumns),
        // get just the first unionAll because we assume that unionAlls have the same structure
        getUnionAll().stream().limit(1).flatMap(SelectClause::getAllColumns)
    ).flatMap(Function.identity());
  }
}
