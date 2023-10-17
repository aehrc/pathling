package au.csiro.pathling.views;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
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
public class ForEachOrNullSelect extends NestedSelectClause {

  /**
   * Same as forEach, but produces a single row with a null value if the collection is empty.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.forEachOrNull">ViewDefinition.select.forEachOrNull</a>
   */
  @NotNull
  String path;

  /**
   * Nested select relative to the {@link #path}.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.select">ViewDefinition.select.select</a>
   */
  @NotNull
  @Size(min = 1)
  List<SelectClause> select;

}
