package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * 'where' filters are FHIRPath expressions joined with an implicit "and". This enables users to
 * select a subset of rows that match a specific need. For example, a user may be interested only in
 * a subset of observations based on code value and can filter them here.
 *
 * @author John Grimes
 */
@Data
public class WhereClause {

  /**
   * The FHIRPath expression for the filter.
   */
  @NotNull
  @SerializedName("expr")
  String expression;

  /**
   * An optional human-readable description of the filter.
   */
  @SerializedName("desc")
  @Nullable
  String description;

}
