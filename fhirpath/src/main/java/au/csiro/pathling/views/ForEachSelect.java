package au.csiro.pathling.views;

import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Same as from, but unnests a new row for each item in the collection.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEach">ViewDefinition.select.forEach</a>
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ForEachSelect extends SelectClause {

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

}
