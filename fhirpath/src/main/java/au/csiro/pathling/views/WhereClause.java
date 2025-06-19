package au.csiro.pathling.views;

import com.google.gson.annotations.SerializedName;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 'where' filters are FHIRPath expressions joined with an implicit "and". This enables users to
 * select a subset of rows that match a specific need. For example, a user may be interested only in
 * a subset of observations based on code value and can filter them here.
 *
 * @author John Grimes
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WhereClause {

  /**
   * The FHIRPath expression for the filter.
   */
  @NotNull
  @SerializedName("path")
  String expression;

  /**
   * An optional human-readable description of the filter.
   */
  @Nullable
  String description;

}
