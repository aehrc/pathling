/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.aliasColumns;

import au.csiro.pathling.QueryHelpers.DatasetWithColumnMap;
import au.csiro.pathling.QueryHelpers.DatasetWithColumns;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Represents any FHIRPath expression which is not a literal.
 *
 * @author John Grimes
 */
@Getter
public abstract class NonLiteralPath implements FhirPath {

  @Nonnull
  protected final String expression;

  @Nonnull
  protected final Dataset<Row> dataset;

  @Nonnull
  protected final Optional<Column> idColumn;

  @Nonnull
  protected final List<Column> valueColumns;

  protected final boolean singular;

  /**
   * Returns an expression representing a resource (other than the subject resource) that this path
   * originated from. This is used in {@code reverseResolve} for joining between the subject
   * resource and a reference within a foreign resource.
   */
  @Nonnull
  protected Optional<ResourcePath> foreignResource;

  /**
   * For paths that traverse from the {@code $this} keyword, this column refers to the values in the
   * collection. This is so that functions that operate over collections can construct a result that
   * is based on the original input using the argument alone, without having to join from the input
   * to the argument (which has problems relating to the generation of duplicate rows).
   */
  @Nonnull
  protected Optional<List<Column>> thisColumns;

  @SuppressWarnings("TypeMayBeWeakened")
  protected NonLiteralPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final List<Column> valueColumns,
      final boolean singular, @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<List<Column>> thisColumns) {
    final DatasetWithColumns datasetAndValues = initializeDatasetAndValues(
        dataset, valueColumns);
    this.expression = expression;
    this.dataset = datasetAndValues.getDataset();
    this.idColumn = idColumn;
    this.valueColumns = datasetAndValues.getColumns();
    this.singular = singular;
    this.foreignResource = foreignResource;
    this.thisColumns = thisColumns;
  }

  /**
   * Gets a this {@link Column} from any of the inputs, if there is one.
   *
   * @param inputs a collection of objects
   * @return a {@link Column}, if one was found
   */
  @Nonnull
  public static Optional<List<Column>> findThisColumns(@Nonnull final Object... inputs) {
    return Stream.of(inputs)
        .filter(input -> input instanceof NonLiteralPath)
        .map(path -> (NonLiteralPath) path)
        .filter(path -> path.getThisColumns().isPresent())
        .findFirst()
        .flatMap(NonLiteralPath::getThisColumns);
  }

  /**
   * Returns the specified child of this path, if there is one.
   *
   * @param name The name of the child element
   * @return an {@link ElementDefinition} object
   */
  @Nonnull
  public abstract Optional<ElementDefinition> getChildElement(@Nonnull final String name);

  @Nonnull
  protected DatasetWithColumns initializeDatasetAndValues(@Nonnull final Dataset<Row> dataset,
      @Nonnull final List<Column> valueColumns) {
    final DatasetWithColumnMap datasetWithColumnMap = aliasColumns(dataset, valueColumns, true);
    final Dataset<Row> aliasedDataset = datasetWithColumnMap.getDataset();
    final List<Column> aliasedValueColumns = datasetWithColumnMap.getMappedColumns(valueColumns);
    return new DatasetWithColumns(aliasedDataset, aliasedValueColumns);
  }

  /**
   * Creates a copy of this NonLiteralPath with an updated {@link Dataset}, ID and value {@link
   * Column}s.
   *
   * @param expression an updated expression to describe the new NonLiteralPath
   * @param dataset the new Dataset that can be used to evaluate this NonLiteralPath against data
   * @param idColumn the new resource identity column
   * @param valueColumns the new expression value columns
   * @param singular the new singular value
   * @param thisColumns a list of columns containing the collection being iterated, for cases where
   * a path is being created to represent the {@code $this} keyword
   * @return a new instance of NonLiteralPath
   */
  @Nonnull
  public abstract NonLiteralPath copy(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
      @Nonnull Optional<Column> idColumn, @Nonnull List<Column> valueColumns, boolean singular,
      @Nonnull Optional<List<Column>> thisColumns);

}
