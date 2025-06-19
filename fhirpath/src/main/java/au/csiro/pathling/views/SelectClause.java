package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.CompatibleUnionColumns;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Defines the 'select' backbone element.
 *
 * @author John Grimes
 * @see <a
 * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#ViewDefinition.select">ViewDefinition.select</a>
 */
@Data
@AllArgsConstructor()
@NoArgsConstructor()
@Builder
public class SelectClause implements SelectionElement {

  /**
   * The customized Lombok builder for {@link SelectClause}. This builder provides convenience
   * methods.
   */
  @SuppressWarnings("unused")
  public static class SelectClauseBuilder {

    /**
     * Convenience method to create columns from a var arg of  {@link Column}.
     */
    public SelectClauseBuilder columns(@Nonnull final Column... columns) {
      return column(List.of(columns));
    }

    /**
     * Convenience method to build select from a var arg of {@link SelectClause}.
     */
    public SelectClauseBuilder selects(@Nonnull final SelectClause... selects) {
      return select(List.of(selects));
    }

    /**
     * Convenience method to create unions from a var arg of {@link SelectClause}.
     */
    public SelectClauseBuilder unionsAll(@Nonnull final SelectClause... unionsAll) {
      return unionAll(List.of(unionsAll));
    }
  }

  /**
   * Creates a new {@link SelectClause} with the given columns.
   *
   * @param columns the columns to include
   * @return a new {@link SelectClause} instance
   */
  @Nonnull
  public static SelectClause ofColumns(@Nonnull final Column... columns) {
    return builder().column(List.of(columns)).build();
  }

  /**
   * Creates a new 'forEach' {@link SelectClause} with the given columns.
   *
   * @param path the path to set in the 'forEach' clause
   * @param columns the columns to include
   * @return a new {@link SelectClause} instance
   */
  @Nonnull
  public static SelectClause forEach(@Nonnull String path, @Nonnull final Column... columns) {
    return builder()
        .forEach(path)
        .column(List.of(columns)).build();
  }


  /**
   * Creates a new 'forEachOrNull' {@link SelectClause} with the given columns.
   *
   * @param path the path to set in the 'forEachOrNull' clause
   * @param columns the columns to include
   * @return a new {@link SelectClause} instance
   */
  @Nonnull
  public static SelectClause forEachOrNull(@Nonnull String path, @Nonnull final Column... columns) {
    return builder()
        .forEachOrNull(path)
        .column(List.of(columns)).build();
  }

  /**
   * A column to be produced in the resulting table. The column is relative to the select structure
   * that contains it.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.column">ViewDefinition.select.column</a>
   */
  @Nonnull
  @NotNull
  @Valid
  @Builder.Default
  List<@Valid Column> column = Collections.emptyList();

  /**
   * Nested select relative to this.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @Nonnull
  @NotNull
  @Size()
  @Valid
  @Builder.Default
  List<@Valid SelectClause> select = Collections.emptyList();

  /**
   * A FHIRPath expression to retrieve the parent element(s) used in the containing select, relative
   * to the root resource or parent `select`, if applicable. `forEach` will produce a row for each
   * element selected in the expression.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEach">ViewDefinition.select.forEach</a>
   */
  @Nullable
  @Builder.Default
  String forEach = null;

  /**
   * Same as forEach, but produces a single row with null values in the nested expression if the
   * collection is empty.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
   */
  @Nullable
  @Builder.Default
  String forEachOrNull = null;

  /**
   * A `unionAll` combines the results of multiple selection structures. Each structure under the
   * `unionAll` must produce the same column names and types.
   *
   * @see <a
   * href="https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.unionAll">ViewDefinition.select.unionAll</a>
   */
  @Nonnull
  @NotNull
  @Size()
  @Valid
  @CompatibleUnionColumns
  @Builder.Default
  List<@Valid SelectClause> unionAll = Collections.emptyList();


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
