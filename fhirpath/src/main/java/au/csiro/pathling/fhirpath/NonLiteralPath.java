/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.QueryHelpers.getUnionableColumns;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.apache.spark.sql.functions.explode_outer;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import java.util.Arrays;
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
  protected final Column idColumn;

  @Nonnull
  protected final Column valueColumn;

  protected final boolean singular;

  /**
   * Returns an expression representing the most current resource that has been navigated to within
   * this path. This is used in {@code reverseResolve} for joining between the subject resource and
   * a reference.
   */
  @Nonnull
  protected Optional<ResourcePath> currentResource;

  /**
   * For paths that traverse from the {@code $this} keyword, this column refers to the values in the
   * collection. This is so that functions that operate over collections can construct a result that
   * is based on the original input using the argument alone, without having to join from the input
   * to the argument (which has problems relating to the generation of duplicate rows).
   */
  @Nonnull
  protected Optional<Column> thisColumn;

  protected NonLiteralPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn) {

    final List<String> datasetColumns = Arrays.asList(dataset.columns());
    checkArgument(datasetColumns.contains(idColumn.toString()),
        "ID column name not present in dataset");
    checkArgument(datasetColumns.contains(valueColumn.toString()),
        "Value column name not present in dataset");
    thisColumn.ifPresent(col -> checkArgument(datasetColumns.contains(col.toString()),
        "$this column name not present in dataset"));

    this.expression = expression;
    this.dataset = dataset;
    this.idColumn = idColumn;
    this.valueColumn = valueColumn;
    this.singular = singular;
    this.currentResource = currentResource;
    this.thisColumn = thisColumn;
  }

  /**
   * Get an ordering {@link Column} from any of the inputs, if there is one.
   *
   * @param inputs a collection of objects
   * @return a {@link Column}, if one was found
   */
  @Nonnull
  public static Optional<Column> findOrderingColumn(@Nonnull final Object... inputs) {
    return Stream.of(inputs)
        .filter(input -> input instanceof NonLiteralPath)
        .map(path -> (NonLiteralPath) path)
        .filter(path -> path.getOrderingColumn().isPresent())
        .findFirst()
        .flatMap(NonLiteralPath::getOrderingColumn);
  }

  /**
   * Gets a this {@link Column} from any of the inputs, if there is one.
   *
   * @param inputs a collection of objects
   * @return a {@link Column}, if one was found
   */
  @Nonnull
  public static Optional<Column> findThisColumn(@Nonnull final Object... inputs) {
    return Stream.of(inputs)
        .filter(input -> input instanceof NonLiteralPath)
        .map(path -> (NonLiteralPath) path)
        .filter(path -> path.getThisColumn().isPresent())
        .findFirst()
        .flatMap(NonLiteralPath::getThisColumn);
  }

  @Nonnull
  @Override
  public Dataset<Row> getOrderedDataset(@Nonnull final Nesting nesting) {
    final Column[] sortColumns = nesting.getOrderingColumns().toArray(new Column[0]);
    return getDataset().orderBy(sortColumns);
  }

  /**
   * Returns the column with the extension container (the _fid to extension values map).
   *
   * @return the column with the extension container.
   */
  @Nonnull
  public Column getExtensionContainerColumn() {
    final ResourcePath rootResource = checkPresent(getCurrentResource(),
        "Current resource missing in traversed path. This is a bug in current resource propagation");
    return rootResource.getExtensionContainerColumn();
  }

  /**
   * Returns the specified child of this path, if there is one.
   *
   * @param name The name of the child element
   * @return an {@link ElementDefinition} object
   */
  @Nonnull
  public abstract Optional<ElementDefinition> getChildElement(@Nonnull final String name);

  /**
   * Creates a copy of this NonLiteralPath with an updated {@link Dataset}, ID and value
   * {@link Column}s.
   *
   * @param expression an updated expression to describe the new NonLiteralPath
   * @param dataset the new Dataset that can be used to evaluate this NonLiteralPath against data
   * @param idColumn the new resource identity column
   * @param valueColumn the new expression value column
   * @param orderingColumn the new ordering column
   * @param singular the new singular value
   * @param thisColumn a list of columns containing the collection being iterated, for cases where a
   * path is being created to represent the {@code $this} keyword
   * @return a new instance of NonLiteralPath
   */
  @Nonnull
  public abstract NonLiteralPath copy(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
      @Nonnull Column idColumn, @Nonnull Column valueColumn,
      @Nonnull Optional<Column> orderingColumn, boolean singular,
      @Nonnull Optional<Column> thisColumn);

  @Nonnull
  @Override
  public FhirPath withExpression(@Nonnull final String expression) {
    return copy(expression, dataset, idColumn, valueColumn, getOrderingColumn(), singular,
        thisColumn);
  }

  @Nonnull
  @Override
  public FhirPath withDataset(@Nonnull final Dataset<Row> dataset) {
    return copy(expression, dataset, idColumn, valueColumn, getOrderingColumn(), singular,
        thisColumn);
  }

  /**
   * Construct a $this path based upon this path.
   *
   * @return a new NonLiteralPath
   */
  @Nonnull
  public NonLiteralPath toThisPath() {
    return copy(NamedFunction.THIS, this.getDataset(), this.getIdColumn(), this.getValueColumn(),
        this.getOrderingColumn(), true, Optional.of(this.getValueColumn()));
  }

  @Override
  public boolean canBeCombinedWith(@Nonnull final FhirPath target) {
    return getClass().equals(target.getClass()) || target instanceof NullLiteralPath;
  }

  @Nonnull
  @Override
  public Dataset<Row> getUnionableDataset(@Nonnull final FhirPath target) {
    return getDataset().select(getUnionableColumns(this, target).toArray(new Column[]{}));
  }

  @Nonnull
  @Override
  public FhirPath unnest() {
    final DatasetWithColumn datasetWithColumn = createColumn(getDataset(),
        explode_outer(getValueColumn()));
    return copy(getExpression(), datasetWithColumn.getDataset(), datasetWithColumn.getColumn(),
        datasetWithColumn.getColumn(), getOrderingColumn(), isSingular(), getThisColumn());
  }
 
}
