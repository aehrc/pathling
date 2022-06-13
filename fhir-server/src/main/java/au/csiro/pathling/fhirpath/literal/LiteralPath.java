/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.QueryHelpers.getUnionableColumns;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents any type of literal expression.
 *
 * @author John Grimes
 */
public abstract class LiteralPath implements FhirPath {

  // See https://hl7.org/fhir/fhirpath.html#types.
  private static final Map<FHIRDefinedType, Class<? extends LiteralPath>> FHIR_TYPE_TO_FHIRPATH_TYPE =
      new ImmutableMap.Builder<FHIRDefinedType, Class<? extends LiteralPath>>()
          .put(FHIRDefinedType.BOOLEAN, BooleanLiteralPath.class)
          .put(FHIRDefinedType.STRING, StringLiteralPath.class)
          .put(FHIRDefinedType.URI, StringLiteralPath.class)
          .put(FHIRDefinedType.CODE, StringLiteralPath.class)
          .put(FHIRDefinedType.OID, StringLiteralPath.class)
          .put(FHIRDefinedType.ID, StringLiteralPath.class)
          .put(FHIRDefinedType.UUID, StringLiteralPath.class)
          .put(FHIRDefinedType.MARKDOWN, StringLiteralPath.class)
          .put(FHIRDefinedType.BASE64BINARY, StringLiteralPath.class)
          .put(FHIRDefinedType.INTEGER, IntegerLiteralPath.class)
          .put(FHIRDefinedType.UNSIGNEDINT, IntegerLiteralPath.class)
          .put(FHIRDefinedType.POSITIVEINT, IntegerLiteralPath.class)
          .put(FHIRDefinedType.DECIMAL, DecimalLiteralPath.class)
          .put(FHIRDefinedType.DATE, DateLiteralPath.class)
          .put(FHIRDefinedType.DATETIME, DateTimeLiteralPath.class)
          .put(FHIRDefinedType.INSTANT, DateTimeLiteralPath.class)
          .put(FHIRDefinedType.TIME, TimeLiteralPath.class)
          .put(FHIRDefinedType.CODING, CodingLiteralPath.class)
          .put(FHIRDefinedType.QUANTITY, QuantityLiteralPath.class)
          .build();

  private static final Map<Class<? extends LiteralPath>, FHIRDefinedType> FHIRPATH_TYPE_TO_FHIR_TYPE =
      new ImmutableMap.Builder<Class<? extends LiteralPath>, FHIRDefinedType>()
          .put(BooleanLiteralPath.class, FHIRDefinedType.BOOLEAN)
          .put(StringLiteralPath.class, FHIRDefinedType.STRING)
          .put(IntegerLiteralPath.class, FHIRDefinedType.INTEGER)
          .put(DecimalLiteralPath.class, FHIRDefinedType.DECIMAL)
          .put(DateLiteralPath.class, FHIRDefinedType.DATE)
          .put(DateTimeLiteralPath.class, FHIRDefinedType.DATETIME)
          .put(TimeLiteralPath.class, FHIRDefinedType.TIME)
          .put(CodingLiteralPath.class, FHIRDefinedType.CODING)
          .put(QuantityLiteralPath.class, FHIRDefinedType.QUANTITY)
          .build();

  @Getter
  @Nonnull
  protected Dataset<Row> dataset;

  @Getter
  @Nonnull
  protected Column idColumn;

  @Getter
  @Nonnull
  protected Column valueColumn;

  /**
   * The HAPI object that represents the value of this literal.
   */
  @Getter
  protected Type literalValue;

  protected LiteralPath(@Nonnull final Dataset<Row> dataset, @Nonnull final Column idColumn,
      @Nonnull final Type literalValue) {
    this.idColumn = idColumn;
    this.literalValue = literalValue;
    this.dataset = dataset;
    this.valueColumn = buildValueColumn();
  }

  /**
   * Builds a FHIRPath literal expression for the provided FHIR object.
   *
   * @param dataset The context dataset to use in building this expression
   * @param idColumn The identity column from the dataset
   * @param literalValue A HAPI FHIR object
   * @return A String representation of the literal in FHIRPath
   */
  @Nonnull
  public static String expressionFor(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Type literalValue) {
    final Class<? extends LiteralPath> literalPathClass = FHIR_TYPE_TO_FHIRPATH_TYPE
        .get(FHIRDefinedType.fromCode(literalValue.fhirType()));
    try {
      final Constructor<? extends LiteralPath> constructor = literalPathClass
          .getDeclaredConstructor(Dataset.class, Column.class, Type.class);
      final LiteralPath literalPath = constructor.newInstance(dataset, idColumn, literalValue);
      return literalPath.getExpression();
    } catch (final NoSuchMethodException | InstantiationException | IllegalAccessException |
                   InvocationTargetException e) {
      throw new RuntimeException("Problem building a LiteralPath class", e);
    }
  }

  @Override
  @Nonnull
  public abstract String getExpression();

  @Override
  public boolean isSingular() {
    return true;
  }

  @Override
  public boolean hasOrder() {
    return true;
  }

  @Nonnull
  @Override
  public Dataset<Row> getOrderedDataset() {
    return getDataset();
  }

  @Nonnull
  @Override
  public Column getOrderingColumn() {
    return ORDERING_NULL_VALUE;
  }

  @Nonnull
  public Column getExtractableColumn() {
    return getValueColumn();
  }

  /**
   * Returns the Java object that represents the value of this literal.
   *
   * @return An Object
   */
  @Nullable
  public abstract Object getJavaValue();

  /**
   * @return A column representing the value for this literal.
   */
  @Nonnull
  public Column buildValueColumn() {
    return lit(getJavaValue());
  }

  /**
   * @param fhirPathClass a subclass of LiteralPath
   * @return a {@link FHIRDefinedType}
   */
  @Nonnull
  private static FHIRDefinedType fhirPathToFhirType(
      @Nonnull final Class<? extends LiteralPath> fhirPathClass) {
    return FHIRPATH_TYPE_TO_FHIR_TYPE.get(fhirPathClass);
  }

  @Nonnull
  @Override
  public FhirPath withExpression(@Nonnull final String expression) {
    return this;
  }

  @Override
  @Nonnull
  public NonLiteralPath combineWith(@Nonnull final FhirPath target,
      @Nonnull final Dataset<Row> dataset, @Nonnull final String expression,
      @Nonnull final Column idColumn, @Nonnull final Optional<Column> eidColumn,
      @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final Optional<Column> thisColumn) {
    if (target instanceof LiteralPath && getClass().equals(target.getClass())) {
      // If the target is another LiteralPath, we can merge it if they have the same FHIR type, as
      // decided by our mapping of literal FHIRPath types to FHIR types.
      final FHIRDefinedType fhirType = fhirPathToFhirType(getClass());
      return ElementPath
          .build(expression, dataset, idColumn, eidColumn, valueColumn, singular, Optional.empty(),
              thisColumn, fhirType);
    } else if (target instanceof ElementPath) {
      // If the target is an ElementPath, we delegate off to the ElementPath to do the merging.
      return target
          .combineWith(this, dataset, expression, idColumn, eidColumn, valueColumn, singular,
              thisColumn);
    }
    // Anything else is invalid.
    throw new InvalidUserInputError(
        "Paths cannot be merged into a collection together: " + getExpression() + ", " + target
            .getExpression());
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
