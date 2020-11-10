/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.literal;

import static au.csiro.pathling.QueryHelpers.ID_COLUMN_SUFFIX;
import static au.csiro.pathling.QueryHelpers.VALUE_COLUMN_SUFFIX;
import static au.csiro.pathling.utilities.Strings.randomShortString;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.QueryHelpers;
import au.csiro.pathling.fhirpath.FhirPath;
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

  @Getter
  @Nonnull
  protected Dataset<Row> dataset;

  @Getter
  @Nonnull
  protected Optional<Column> idColumn;

  @Getter
  @Nonnull
  protected Column valueColumn;

  /**
   * The HAPI object that represents the value of this literal.
   */
  @Getter
  protected Type literalValue;

  protected LiteralPath(@Nonnull final Dataset<Row> dataset,
      @Nonnull final Optional<Column> idColumn, @Nonnull final Type literalValue) {
    final String hash = randomShortString();
    final String idColumnName = hash + ID_COLUMN_SUFFIX;
    final String valueColumnName = hash + VALUE_COLUMN_SUFFIX;

    this.literalValue = literalValue;
    final Column valueColumn = buildValueColumn();

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
    this.dataset = QueryHelpers.applySelection(hashedDataset, this.idColumn);
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
      @Nonnull final Optional<Column> idColumn, @Nonnull final Type literalValue) {
    final Class<? extends LiteralPath> literalPathClass = FHIR_TYPE_TO_FHIRPATH_TYPE
        .get(FHIRDefinedType.fromCode(literalValue.fhirType()));
    try {
      final Constructor<? extends LiteralPath> constructor = literalPathClass
          .getDeclaredConstructor(Dataset.class, Optional.class, Type.class);
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

}
