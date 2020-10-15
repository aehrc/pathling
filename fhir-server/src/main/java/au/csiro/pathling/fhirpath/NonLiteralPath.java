/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import static au.csiro.pathling.QueryHelpers.ID_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.VALUE_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.applySelection;
import static au.csiro.pathling.utilities.Strings.randomShortString;

import au.csiro.pathling.fhirpath.element.ElementDefinition;
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
      @Nonnull final Optional<Column> idColumn, @Nonnull final Column valueColumn,
      final boolean singular, @Nonnull final Optional<ResourcePath> foreignResource,
      @Nonnull final Optional<Column> thisColumn) {
    this.expression = expression;
    this.singular = singular;
    this.foreignResource = foreignResource;
    this.thisColumn = thisColumn;

    final String hash = randomShortString();
    final String idColumnName = hash + ID_COLUMN_SUFFIX;
    final String valueColumnName = hash + VALUE_COLUMN_SUFFIX;

    Dataset<Row> hashedDataset = dataset;
    if (idColumn.isPresent()) {
      hashedDataset = dataset.withColumn(idColumnName, idColumn.get());
    }
    hashedDataset = hashedDataset.withColumn(valueColumnName, valueColumn);

    if (idColumn.isPresent()) {
      this.idColumn = Optional.of(hashedDataset.col(idColumnName));
    } else {
      this.idColumn = Optional.empty();
    }
    this.valueColumn = hashedDataset.col(valueColumnName);
    this.dataset = applySelection(hashedDataset, this.idColumn);
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
   * Creates a copy of this NonLiteralPath with an updated {@link Dataset}, ID and value {@link
   * Column}s.
   *
   * @param expression an updated expression to describe the new NonLiteralPath
   * @param dataset the new Dataset that can be used to evaluate this NonLiteralPath against data
   * @param idColumn the new resource identity column
   * @param valueColumn the new expression value column
   * @param singular the new singular value
   * @param thisColumn a column containing the collection being iterated, for cases where a path is
   * being created to represent the {@code $this} keyword
   * @return a new instance of NonLiteralPath
   */
  @Nonnull
  public abstract NonLiteralPath copy(@Nonnull String expression, @Nonnull Dataset<Row> dataset,
      @Nonnull Optional<Column> idColumn, @Nonnull Column valueColumn, boolean singular,
      @Nonnull Optional<Column> thisColumn);

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

}
