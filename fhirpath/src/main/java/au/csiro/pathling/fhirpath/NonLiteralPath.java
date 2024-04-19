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
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.posexplode_outer;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.literal.NullLiteralPath;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.commons.lang3.tuple.MutablePair;
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

  private static final String THIS_ORDERING_COLUMN_NAME = "eid";
  private static final String THIS_VALUE_COLUMN_NAME = "value";

  @Nonnull
  protected final String expression;

  @Nonnull
  protected final Dataset<Row> dataset;

  @Nonnull
  protected final Column idColumn;

  /**
   * A {@link Column} that represents the unique ID for an element within a collection.
   */
  @Nonnull
  protected final Optional<Column> eidColumn;

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
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> currentResource,
      @Nonnull final Optional<Column> thisColumn) {

    final List<String> datasetColumns = Arrays.asList(dataset.columns());
    checkArgument(datasetColumns.contains(idColumn.toString()),
        "ID column name not present in dataset");
    checkArgument(datasetColumns.contains(valueColumn.toString()),
        "Value column name not present in dataset");
    thisColumn.ifPresent(col -> checkArgument(datasetColumns.contains(col.toString()),
        "$this column name not present in dataset"));
    eidColumn.ifPresent(col -> checkArgument(datasetColumns.contains(col.toString()),
        "eid column name not present in dataset"));

    this.expression = expression;
    this.dataset = dataset;
    this.idColumn = idColumn;
    this.eidColumn = eidColumn;
    this.valueColumn = valueColumn;
    this.singular = singular;
    this.currentResource = currentResource;
    this.thisColumn = thisColumn;
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

  @Override
  public boolean hasOrder() {
    return isSingular() || eidColumn.isPresent();
  }

  @Nonnull
  @Override
  public Dataset<Row> getOrderedDataset() {
    checkHasOrder();
    return eidColumn.map(c -> getDataset().orderBy(c)).orElse(getDataset());
  }

  @Nonnull
  @Override
  public Column getOrderingColumn() {
    checkHasOrder();
    return eidColumn.orElse(ORDERING_NULL_VALUE);
  }

  @Nonnull
  public Column getExtractableColumn() {
    return getValueColumn();
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
   * Get an element ID {@link Column} from any of the inputs, if there is one.
   *
   * @param inputs a collection of objects
   * @return a {@link Column}, if one was found
   */
  @Nonnull
  public static Optional<Column> findEidColumn(@Nonnull final Object... inputs) {
    return Stream.of(inputs)
        .filter(input -> input instanceof NonLiteralPath)
        .map(path -> (NonLiteralPath) path)
        .filter(path -> path.getEidColumn().isPresent())
        .findFirst()
        .flatMap(NonLiteralPath::getEidColumn);
  }

  /**
   * Creates a copy of this NonLiteralPath with an updated {@link Dataset}, ID and value
   * {@link Column}s.
   *
   * @param expression an updated expression to describe the new NonLiteralPath
   * @param dataset the new Dataset that can be used to evaluate this NonLiteralPath against data
   * @param idColumn the new resource identity column
   * @param eidColumn the new element identity column
   * @param valueColumn the new expression value column
   * @param singular the new singular value
   * @param thisColumn a list of columns containing the collection being iterated, for cases where a
   * path is being created to represent the {@code $this} keyword
   * @return a new instance of NonLiteralPath
   */
  @Nonnull
  public abstract NonLiteralPath copy(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
      @Nonnull Column idColumn, @Nonnull Optional<Column> eidColumn, @Nonnull Column valueColumn,
      boolean singular, @Nonnull Optional<Column> thisColumn);

  @Nonnull
  @Override
  public FhirPath withExpression(@Nonnull final String expression) {
    return copy(expression, dataset, idColumn, eidColumn, valueColumn, singular, thisColumn);
  }

  /**
   * Construct a $this path based upon this path.
   *
   * @return a new NonLiteralPath
   */
  @Nonnull
  public NonLiteralPath toThisPath() {
    final DatasetWithColumn inputWithThis = createColumn(
        this.getDataset(), this.makeThisColumn());

    return copy(NamedFunction.THIS, inputWithThis.getDataset(), this.getIdColumn(),
        this.getEidColumn(), this.getValueColumn(), true,
        Optional.of(inputWithThis.getColumn()));
  }

  @Nonnull
  public Optional<Column> getThisOrderingColumn() {
    return getThisColumn().map(thisColumn -> thisColumn.getField(THIS_ORDERING_COLUMN_NAME));
  }

  @Nonnull
  public Optional<Column> getThisValueColumn() {
    return getThisColumn().map(thisColumn -> thisColumn.getField(THIS_VALUE_COLUMN_NAME));
  }

  /**
   * Constructs a $this column for this path as a structure with two fields: `eid` and `value`.
   *
   * @return a new {@link Column}
   */
  @Nonnull
  private Column makeThisColumn() {
    return struct(
        getOrderingColumn().alias(THIS_ORDERING_COLUMN_NAME),
        getValueColumn().alias(THIS_VALUE_COLUMN_NAME));
  }

  /**
   * Constructs the new value of the element ID column, based on its current value in the parent
   * path and the index of the element in the child path.
   * <p>
   * If the parent's eid is None it indicates that the parent is singular and the new eid needs to
   * be created based on the value of the indexColumn:
   * <ul>
   * <li>if the indexColumns is null then the eid can be set to null.</li>
   * <li>otherwise it should be a one element array with the indexColumn value.</li>
   * </ul>
   * <p>
   * If the parent eid exists then the value of the index column needs to be appended to the
   * existing id.
   * <ul>
   * <li>if the existing eid is null then the index must be null as well and the new id should be
   * null.</li>
   * <li>otherwise the existing eid needs to be extended with the value of indexColumn or 0 if index
   * column is null.</li>
   * </ul>
   *
   * @param indexColumn the {@link Column} with the child path element index
   * @return the element ID {@link Column} for the child path.
   */
  @Nonnull
  public Column expandEid(@Nonnull final Column indexColumn) {
    final Column indexOrZero =
        when(indexColumn.isNotNull(), array(indexColumn))
            .otherwise(array(lit(0)));
    final Column indexOrNull =
        when(indexColumn.isNotNull(), array(indexColumn))
            .otherwise(ORDERING_NULL_VALUE);
    return getEidColumn()
        .map(eid -> when(eid.isNull(), ORDERING_NULL_VALUE).otherwise(
            concat(eid, indexOrZero))).orElse(indexOrNull);
  }

  /**
   * Explodes an array column from a provided dataset preserving all the columns from this one and
   * producing updated element ids.
   *
   * @param arrayDataset the dataset containing the array column. It should also contain all columns
   * from this dataset.
   * @param arrayCol the array column to explode.
   * @param outValueAndEidCols the output pair of columns: `left` is set to the new value column and
   * `right` to the new eid column.
   * @return the {@link Dataset} with the exploded array.
   */
  @Nonnull
  public Dataset<Row> explodeArray(@Nonnull final Dataset<Row> arrayDataset,
      @Nonnull final Column arrayCol,
      @Nonnull final MutablePair<Column, Column> outValueAndEidCols) {
    final Column[] allColumns = Stream.concat(Arrays.stream(dataset.columns())
            .map(dataset::col), Stream
            .of(posexplode_outer(arrayCol)
                .as(new String[]{"index", "value"})))
        .toArray(Column[]::new);
    final Dataset<Row> resultDataset = arrayDataset.select(allColumns);
    outValueAndEidCols.setLeft(resultDataset.col("value"));
    outValueAndEidCols.setRight(expandEid(resultDataset.col("index")));
    return resultDataset;
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

}
