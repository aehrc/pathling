package au.csiro.pathling.fhirpath.execution;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;

/**
 * Represents the result of evaluating a FHIRPath expression to a collection with an optional
 * dataset.
 */
@Value
public class EvaluatedPath {

  @Nonnull
  FhirPath path;

  @Nonnull
  Collection result;

  @Nullable
  Dataset<Row> dataset;

  @Nonnull
  public String toExpression() {
    return path.toExpression();
  }

  @Nonnull
  public Column getColumnValue() {
    return result.getColumnValue();
  }

  /**
   * Returns whether the result of this path is a singular value.
   *
   * @return true if the result is singular, false if it is an array
   */
  public boolean isSingular() {
    if (dataset == null) {
      throw new IllegalStateException("Cannot determine singularity without a dataset");
    }
    return !(dataset.select(getColumnValue()).schema().fields()[0].dataType() instanceof ArrayType);
  }

  /**
   * Binds this evaluated path to a dataset.
   *
   * @param dataset the dataset to bind to
   * @return a new {@link EvaluatedPath} with the dataset bound
   */
  @Nonnull
  public EvaluatedPath bind(@Nonnull final Dataset<Row> dataset) {
    return new EvaluatedPath(path, result, dataset);
  }

  /**
   * Creates a new {@link EvaluatedPath} with the given path and result.
   *
   * @param path the path
   * @param result the result
   * @return a new unbound {@link EvaluatedPath}
   */
  @Nonnull
  public static EvaluatedPath of(@Nonnull final FhirPath path, @Nonnull final Collection result) {
    return new EvaluatedPath(path, result, null);
  }
}
