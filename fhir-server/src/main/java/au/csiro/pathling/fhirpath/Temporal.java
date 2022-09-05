/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.Numeric.MathOperation;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

public interface Temporal {

  @Nonnull
  Function<QuantityLiteralPath, FhirPath> getDateArithmeticOperation(
      @Nonnull MathOperation operation, @Nonnull Dataset<Row> dataset,
      @Nonnull String expression);

  @Nonnull
  static Function<QuantityLiteralPath, FhirPath> buildDateArithmeticOperation(
      @Nonnull final FhirPath source, final @Nonnull MathOperation operation,
      final @Nonnull Dataset<Row> dataset, final @Nonnull String expression,
      final String additionFunctionName, final String subtractionFunctionName) {
    return target -> {
      final String functionName;
      final Optional<Column> eidColumn = NonLiteralPath.findEidColumn(source, target);
      final Optional<Column> thisColumn = NonLiteralPath.findThisColumn(source, target);

      switch (operation) {
        case ADDITION:
          functionName = additionFunctionName;
          break;
        case SUBTRACTION:
          functionName = subtractionFunctionName;
          break;
        default:
          throw new AssertionError("Unsupported date arithmetic operation: " + operation);
      }

      final Column valueColumn = functions.callUDF(functionName, source.getValueColumn(),
          target.getValueColumn());
      return ElementPath.build(expression, dataset, source.getIdColumn(), eidColumn, valueColumn,
          true,
          Optional.empty(), thisColumn, FHIRDefinedType.DATETIME);
    };
  }

}