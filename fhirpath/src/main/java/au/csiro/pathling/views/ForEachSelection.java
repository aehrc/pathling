package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * A 'forEach' expression unnests a new row for each item in the specified FHIRPath expression, and
 * users will select columns in the nested select. This differs from the 'from' expression above
 * because it creates a new row for each item in the matched collection, unrolling that part of the
 * resource.
 *
 * @author John Grimes
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class ForEachSelection extends NestedSelectClause {

  /**
   * The FHIRPath expression to unnest.
   */
  @NotNull
  @SerializedName("forEach")
  String expression;

  /**
   * The nested select clauses.
   */
  @NotNull
  @Size(min = 1)
  List<SelectClause> select;

}
