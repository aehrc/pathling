package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.CompatibleUnionColumns;
import com.google.gson.annotations.SerializedName;
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
 * Same as from, but unnests a new row for each item in the collection.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEach">ViewDefinition.select.forEach</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = false)
public class ForEachSelect extends SelectClause {

  /**
   * The customized Lombok builder for {@link ForEachSelect}. This builder provides convenience
   * methods.
   */
  @SuppressWarnings("unused")
  public static class ForEachSelectBuilder {

    /**
     * Convenience method to create columns from a var arg of {@link Column}.
     */
    public ForEachSelectBuilder columns(@Nonnull final Column... columns) {
      return column(List.of(columns));
    }

    /**
     * Convenience method to build select from a var arg of {@link SelectClause}.
     */
    public ForEachSelectBuilder selects(@Nonnull final SelectClause... selects) {
      return select(List.of(selects));
    }

    /**
     * Convenience method to create unions from a var arg of {@link SelectClause}.
     */
    public ForEachSelectBuilder unionsAll(@Nonnull final SelectClause... unionsAll) {
      return unionAll(List.of(unionsAll));
    }
  }

  /**
   * Creates a builder with the path already set.
   *
   * @param path the path to set
   * @return a builder with the path set
   */
  @Nonnull
  public static ForEachSelectBuilder ofPath(@Nonnull final String path) {
    return builder().path(path);
  }

  /**
   * Same as from, but unnests a new row for each item in the collection.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEach">ViewDefinition.select.forEach</a>
   */
  @NotNull
  @SerializedName("forEach")
  String path;

  @NotNull
  @Valid
  @Builder.Default
  List<@Valid Column> column = Collections.emptyList();

  /**
   * Nested select relative to the {@link #path}.
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
