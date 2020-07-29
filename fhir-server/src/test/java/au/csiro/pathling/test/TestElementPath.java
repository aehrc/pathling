/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test;

import static au.csiro.pathling.test.assertions.Assertions.assertTrue;
import static au.csiro.pathling.test.helpers.SparkHelpers.getIdAndValueColumns;

import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class TestElementPath extends ElementPath {

  public static final String ORIGIN_COLUMN = "origin";

  protected TestElementPath(@Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column idColumn, @Nonnull final Column valueColumn, final boolean singular,
      @Nonnull final FHIRDefinedType fhirType) {
    super(expression, dataset, idColumn, valueColumn, singular, fhirType);
  }

  @Nonnull
  public static ElementPath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, final boolean singular,
      @Nonnull final ResourceDefinition originType, @Nonnull final ElementDefinition definition) {
    try {
      final ElementPath elementPath = TestElementPath
          .build(expression, dataset, singular, definition);
      final Method setOriginColumnMethod = NonLiteralPath.class
          .getDeclaredMethod("setOriginColumn", Optional.class);
      final Method setOriginTypeMethod = NonLiteralPath.class
          .getDeclaredMethod("setOriginType", Optional.class);
      setOriginColumnMethod.setAccessible(true);
      setOriginTypeMethod.setAccessible(true);
      setOriginColumnMethod.invoke(elementPath, Optional.of(dataset.col(ORIGIN_COLUMN)));
      setOriginTypeMethod.invoke(elementPath, Optional.of(originType));
      return elementPath;
    } catch (final NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Problem creating TestElementPath", e);
    }
  }

  @Nonnull
  public static ElementPath build(@Nonnull final String expression,
      @Nonnull final Dataset<Row> dataset, final boolean singular,
      @Nonnull final ElementDefinition definition) {
    final IdAndValueColumns idAndValueColumns = getIdAndValueColumns(dataset);
    final Optional<FHIRDefinedType> fhirType = definition.getFhirType();
    assertTrue(fhirType.isPresent());
    try {
      final Method elementPathConstructor = ElementPath.class
          .getDeclaredMethod("getInstance", String.class, Dataset.class, Column.class, Column.class,
              boolean.class, FHIRDefinedType.class);
      final Field definitionField = ElementPath.class.getDeclaredField("definition");
      elementPathConstructor.setAccessible(true);
      definitionField.setAccessible(true);

      final ElementPath path = (ElementPath) elementPathConstructor
          .invoke(ElementPath.class, expression, dataset, idAndValueColumns.getId(),
              idAndValueColumns.getValue(), singular, fhirType.get());
      definitionField.set(path, Optional.of(definition));
      return path;
    } catch (final InvocationTargetException | NoSuchMethodException | IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException("Problem creating TestElementPath", e);
    }
  }
}
