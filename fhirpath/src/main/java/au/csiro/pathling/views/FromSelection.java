package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * A 'from' expression is a convenience to select values relative to some parent FHIRPath. This does
 * not unnest or unroll multiple values. If the 'from' results in a FHIRPath collection, that full
 * collection is used in the nested select, so the resulting view would have repeated fields rather
 * than a separate row per value.
 *
 * @author John Grimes
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class FromSelection extends NestedSelectClause {

  /**
   * The FHIRPath expression for the parent path to select from.
   */
  @NotNull
  @SerializedName("from")
  String expression;

  /**
   * The nested select clauses.
   */
  @NotNull
  List<SelectClause> select;

}
