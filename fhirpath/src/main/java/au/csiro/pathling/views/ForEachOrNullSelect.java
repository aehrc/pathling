package au.csiro.pathling.views;

import au.csiro.pathling.views.validation.CompatibleUnionColumns;
import com.google.gson.annotations.SerializedName;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Same as forEach, but produces a single row with a null value if the collection is empty.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ForEachOrNullSelect extends SelectClause {

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
  List<Column> column = Collections.emptyList();

  /**
   * Nested select relative to the {@link #path}.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @NotNull
  @Size()
  List<SelectClause> select = Collections.emptyList();

  @NotNull
  @Size()
  @CompatibleUnionColumns
  List<SelectClause> unionAll = Collections.emptyList();

}
