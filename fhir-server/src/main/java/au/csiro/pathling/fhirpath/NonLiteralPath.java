/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

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
  protected Optional<Column> thisColumn;

  protected NonLiteralPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<ResourcePath> foreignResource,
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
    this.foreignResource = foreignResource;
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
   * Creates a copy of this NonLiteralPath with an updated {@link Dataset}, ID and value {@link
   * Column}s.
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
    return functions.struct(
        getOrderingColumn().alias(THIS_ORDERING_COLUMN_NAME),
        getValueColumn().alias(THIS_VALUE_COLUMN_NAME));
  }

  /**
   * Construct the new value of the element ID column, based on its current value in the parent path
   * and the index of an element in the child path.
   *
   * @param indexColumn the {@link Column} with the child path element index
   * @return an element ID Column for the child path
   */
  @Nonnull
  public Column expandEid(@Nonnull final Column indexColumn) {
    final Column indexAsArray = functions.array(indexColumn);
    final Column compositeEid = getEidColumn()
        .map(eid -> functions.when(eid.isNull(), ORDERING_NULL_VALUE).otherwise(
            functions.concat(eid, indexAsArray))).orElse(indexAsArray);

    return functions.when(indexColumn.isNull(), ORDERING_NULL_VALUE)
        .otherwise(compositeEid);
  }

}
