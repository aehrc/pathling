package au.csiro.pathling.views;

import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * The result of each selection within the union will be combined according to the semantics of the
 * union operator in FHIRPath. The results of the selected expressions must be of the same type, or
 * able to be implicitly converted to a common type according to the FHIRPath data type conversion
 * rules.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.union">ViewDefinition.select.union</a>
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class UnionSelection extends NestedSelectClause {

  /**
   * The result of each selection within the union will be combined according to the semantics of
   * the union operator in FHIRPath. The results of the selected expressions must be of the same
   * type, or able to be implicitly converted to a common type according to the FHIRPath data type
   * conversion rules.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.union">ViewDefinition.select.union</a>
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
