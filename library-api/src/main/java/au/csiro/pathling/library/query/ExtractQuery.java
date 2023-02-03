package au.csiro.pathling.library.query;

import static au.csiro.pathling.utilities.Preconditions.requireNonBlank;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.extract.ExtractRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.extract.ExtractRequest.ExpressionWithLabel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents an extract query.
 *
 * @author Piotr Szul
 */
public class ExtractQuery {

  @Nullable
  private PathlingClient pathlingClient = null;

  @Nonnull
  final ResourceType subjectResource;

  @Nonnull
  final List<ExpressionWithLabel> columnsWithLabels = new ArrayList<>();

  @Nonnull
  final List<String> filters = new ArrayList<>();

  @Nonnull
  Optional<Integer> limit = Optional.empty();

  private ExtractQuery(@Nonnull final ResourceType subjectResource) {
    this.subjectResource = subjectResource;
  }

  /**
   * Binds the query to a specific client.
   *
   * @param pathlingClient the client to use.
   * @return this query
   */
  @Nonnull
  public ExtractQuery withClient(@Nonnull final PathlingClient pathlingClient) {
    this.pathlingClient = pathlingClient;
    return this;
  }

  /**
   * Sets the limit on the number of rows returned in the extract result.
   *
   * @param limit the upper limit on the number of rows in the result.
   * @return this query.
   */
  @Nonnull
  public ExtractQuery withLimit(int limit) {
    this.limit = Optional.of(limit);
    return this;
  }

  /**
   * Adds a fhirpath filter expression to the query. The extract query result only include rows for
   * resources that match ALL the filters.
   *
   * @param filterFhirpath the filter expression to add.
   * @return this query.
   */
  @Nonnull
  public ExtractQuery withFilter(@Nonnull final String filterFhirpath) {
    filters.add(requireNonBlank(filterFhirpath, "Filter expression cannot be blank"));
    return this;
  }

  /**
   * Adds a fhirpath expression that represents a column to be extract in the result.
   *
   * @param columnFhirpath the column expressions.
   * @return this query.
   */
  @Nonnull
  public ExtractQuery withColumn(@Nonnull final String columnFhirpath) {
    columnsWithLabels.add(ExpressionWithLabel.withExpressionAsLabel(columnFhirpath));
    return this;
  }

  /**
   * Adds a fhirpath expression that represents a column to be extract in the result with the
   * explict label.
   *
   * @param columnFhirpath the column expressions.
   * @param label the label of the column.
   * @return this query.
   */
  @Nonnull
  public ExtractQuery withColumn(@Nonnull final String columnFhirpath,
      @Nonnull final String label) {
    columnsWithLabels.add(ExpressionWithLabel.of(columnFhirpath, label));
    return this;
  }

  /**
   * Executes the query on the bound client.
   *
   * @return the dataset with the result of the query.
   */
  @Nonnull
  public Dataset<Row> execute() {
    return requireNonNull(this.pathlingClient).execute(buildRequest());
  }

  /**
   * Executes the query on the given client.
   *
   * @param pathlingClient the client to execute the query against.
   * @return the dataset with the result of the query.
   */
  @Nonnull
  public Dataset<Row> execute(@Nonnull final PathlingClient pathlingClient) {
    return pathlingClient.execute(buildRequest());
  }

  /**
   * Construct a new extract query instance for the given subject resource type.
   *
   * @param subjectResourceType the type of the subject resource.
   * @return the new instance of (unbound) extract query.
   */
  @Nonnull
  public static ExtractQuery of(@Nonnull final ResourceType subjectResourceType) {
    return new ExtractQuery(subjectResourceType);
  }

  @Nonnull
  private static <T> List<T> normalizedList(@Nonnull List<T> list) {
    return list.isEmpty()
           ? Collections.emptyList()
           : list;
  }

  @Nonnull
  private ExtractRequest buildRequest() {
    return new ExtractRequest(subjectResource,
        normalizedList(columnsWithLabels),
        normalizedList(filters),
        limit);
  }
}
