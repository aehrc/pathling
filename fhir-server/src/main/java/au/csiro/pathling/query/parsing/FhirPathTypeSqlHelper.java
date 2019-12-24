package au.csiro.pathling.query.parsing;

import java.util.function.BiFunction;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Type;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;

public interface FhirPathTypeSqlHelper {
  Column getLiteralColumn(Type litValue);

  BiFunction<Column, Column, Column> getEquality();

  static FhirPathTypeSqlHelper forType(FhirPathType pathType) {
    if (pathType.isPrimitive()) {
      return PrimitiveFhirPathTypeSqlHelper.INSTANCE;
    } else if (FhirPathType.CODING.equals(pathType)) {
      return CodingFhirPathTypeSqlHelper.INSTANCE;
    } else {
      throw new IllegalArgumentException(
          "Cannot construct SQLHelper for FhirPathType: " + pathType);
    }
  }
}
