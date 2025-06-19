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
 * Same as forEach, but produces a single row with a null value if the collection is empty.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = false)
public class ForEachOrNullSelect extends SelectClause {

  /**
   * The customized Lombok builder for {@link ForEachOrNullSelect}. This builder provides convenience
   * methods.
   */
  @SuppressWarnings("unused")
  public static class ForEachOrNullSelectBuilder {

    /**
     * Convenience method to create columns from a var arg of {@link Column}.
     */
    public ForEachOrNullSelectBuilder columns(@Nonnull final Column... columns) {
      return column(List.of(columns));
    }

    /**
     * Convenience method to build select from a var arg of {@link SelectClause}.
     */
    public ForEachOrNullSelectBuilder selects(@Nonnull final SelectClause... selects) {
      return select(List.of(selects));
    }

    /**
     * Convenience method to create unions from a var arg of {@link SelectClause}.
     */
    public ForEachOrNullSelectBuilder unionsAll(@Nonnull final SelectClause... unionsAll) {
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
  public static ForEachOrNullSelectBuilder withPath(@Nonnull final String path) {
    return builder().path(path);
  }

  /**
   * Creates a new {@link ForEachOrNullSelect} with the given path and columns.
   *
   * @param path the path to set
   * @param columns the columns to include
   * @return a new {@link ForEachOrNullSelect} instance
   */
  @Nonnull
  public static ForEachOrNullSelect ofColumns(@Nonnull final String path, @Nonnull final Column... columns) {
    return builder().path(path).column(List.of(columns)).build();
  }

  /**
   * Same as forEach, but produces a single row with a null value if the collection is empty.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
   */
  @NotNull
  @SerializedName("forEachOrNull")
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
