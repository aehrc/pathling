package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.CompatibleUnionColumns;
import jakarta.annotation.Nonnull;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Creates a scope for selection relative to a parent FHIRPath expression.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.from">ViewDefinition.select.from</a>
 */
@Data
@AllArgsConstructor()
@NoArgsConstructor()
@Builder
@EqualsAndHashCode(callSuper = false)
public class ColumnSelect extends SelectClause {

  /**
   * The customized Lombok builder for {@link ColumnSelect}. This builder provides convenience
   * methods.
   */
  @SuppressWarnings("unused")
  public static class ColumnSelectBuilder {

    /**
     * Convenience method to create columns from a var arg of  {@link Column}.
     */
    public ColumnSelectBuilder columns(@Nonnull final Column... columns) {
      return column(List.of(columns));
    }

    /**
     * Convenience method to build select from a var arg of {@link SelectClause}.
     */
    public ColumnSelectBuilder selects(@Nonnull final SelectClause... selects) {
      return select(List.of(selects));
    }

    /**
     * Convenience method to create unions from a var arg of {@link SelectClause}.
     */
    public ColumnSelectBuilder unionsAll(@Nonnull final SelectClause... unionsAll) {
      return unionAll(List.of(unionsAll));
    }
  }

  /**
   * Creates a new {@link ColumnSelect} with the given columns.
   *
   * @param columns the columns to include
   * @return a new {@link ColumnSelect} instance
   */
  @Nonnull
  public static ColumnSelect ofColumns(@Nonnull final Column... columns) {
    return builder().column(List.of(columns)).build();
  }


  @NotNull
  @Valid
  @Builder.Default
  List<@Valid Column> column = Collections.emptyList();

  /**
   * Nested select relative to this.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @NotNull
  @Size()
  @Valid
  @Builder.Default
  List<@Valid SelectClause> select = Collections.emptyList();

  @NotNull
  @Size()
  @Valid
  @CompatibleUnionColumns
  @Builder.Default
  List<@Valid SelectClause> unionAll = Collections.emptyList();
}
