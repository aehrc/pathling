package au.csiro.pathling.views;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builder class for creating {@link SelectClause} instances.
 * <p>
 * This builder provides a fluent interface for constructing select clauses with various components
 * such as columns, nested selects, forEach operations, and union operations. The builder follows
 * the builder pattern, allowing method chaining for convenient configuration.
 * <p>
 * Select clauses define what data should be extracted from FHIR resources and how it should be
 * structured. They can include simple column extractions, nested selections, and operations that
 * iterate over collections within FHIR resources.
 * <p>
 * Usage example:
 * <pre>{@code
 * SelectClause clause = new SelectClauseBuilder()
 *     .column(Column.of("id", "id"))
 *     .column(Column.of("name", "name.given"))
 *     .forEach("address")
 *     .select(SelectClause.of("city", "city"))
 *     .build();
 * }</pre>
 *
 * @author John Grimes
 * @see SelectClause
 * @see Column
 * @see FhirViewBuilder
 */
public class SelectClauseBuilder {

  @Nonnull
  private final List<Column> column = new ArrayList<>();

  @Nonnull
  private final List<SelectClause> select = new ArrayList<>();

  @Nullable
  private String forEach;

  @Nullable
  private String forEachOrNull;

  @Nonnull
  private final List<SelectClause> unionAll = new ArrayList<>();

  /**
   * Adds one or more columns to the select clause.
   * <p>
   * Columns define individual data extractions from FHIR resources. Each column typically consists
   * of a name and a FHIRPath expression that specifies how to extract the data.
   *
   * @param column one or more columns to add to the select clause
   * @return this builder instance for method chaining
   */
  public SelectClauseBuilder column(@Nonnull final Column... column) {
    Collections.addAll(this.column, column);
    return this;
  }

  /**
   * Adds one or more nested select clauses.
   * <p>
   * Nested select clauses allow for hierarchical data extraction, enabling the creation of complex
   * data structures from FHIR resources. These are typically used in conjunction with forEach
   * operations to process collections.
   *
   * @param select one or more nested select clauses to add
   * @return this builder instance for method chaining
   */
  public SelectClauseBuilder select(@Nonnull final SelectClause... select) {
    Collections.addAll(this.select, select);
    return this;
  }

  /**
   * Sets the forEach expression for iterating over collections.
   * <p>
   * The forEach operation allows the select clause to iterate over collections within FHIR
   * resources. For each item in the collection, the nested select clauses and columns will be
   * evaluated. This is useful for extracting data from arrays like addresses, contact points, or
   * identifiers.
   *
   * @param forEach the FHIRPath expression that identifies the collection to iterate over
   * @return this builder instance for method chaining
   */
  public SelectClauseBuilder forEach(@Nonnull final String forEach) {
    this.forEach = forEach;
    return this;
  }

  /**
   * Sets the forEachOrNull expression for iterating over collections with null handling.
   * <p>
   * Similar to forEach, but handles cases where the collection might be null or empty. If the
   * collection is null or empty, this will produce a single row with null values rather than no
   * rows at all. This is useful for maintaining consistent output structure even when optional
   * collections are absent.
   *
   * @param forEachOrNull the FHIRPath expression that identifies the collection to iterate over
   * @return this builder instance for method chaining
   */
  public SelectClauseBuilder forEachOrNull(@Nonnull final String forEachOrNull) {
    this.forEachOrNull = forEachOrNull;
    return this;
  }

  /**
   * Adds select clauses to be combined using a union operation.
   * <p>
   * The {@code unionAll} operation combines the results of multiple select clauses into a single
   * result set. This is useful for combining data from different parts of a FHIR resource.
   *
   * @param selects one or more select clauses to union with the main select
   * @return this builder instance for method chaining
   */
  public SelectClauseBuilder unionAll(@Nonnull final SelectClause... selects) {
    Collections.addAll(this.unionAll, selects);
    return this;
  }

  /**
   * Builds and returns a new {@link SelectClause} instance with the configured settings.
   * <p>
   * This method creates the final select clause object using all the components that have been
   * configured through the builder methods. The resulting select clause can be used in FHIR views
   * to define data extraction logic.
   *
   * @return a new SelectClause instance configured with the builder's settings
   */
  @Nonnull
  public SelectClause build() {
    return new SelectClause(column, select, forEach, forEachOrNull, unionAll);
  }

}
