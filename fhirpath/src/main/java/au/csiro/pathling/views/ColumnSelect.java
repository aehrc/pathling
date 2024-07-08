package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Creates a scope for selection relative to a parent FHIRPath expression.
 *
 * @author John Grimes
 * @see <a
 * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.from">ViewDefinition.select.from</a>
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ColumnSelect extends SelectClause {

  /**
   * Creates a scope for selection relative to a parent FHIRPath expression.
   *
   * @see <a
   * href="https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition-definitions.html#diff_ViewDefinition.select.from">ViewDefinition.select.from</a>
   */
  @Nullable
  @SerializedName("from")
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
  List<SelectClause> select = Collections.emptyList();

  @NotNull
  @Size()
  List<SelectClause> unionAll = Collections.emptyList();
}
