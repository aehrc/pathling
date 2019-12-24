package au.csiro.pathling.query.parsing;

import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.Type;

import static org.apache.spark.sql.functions.*;
import java.util.function.BiFunction;

public class PrimitiveFhirPathTypeSqlHelper implements FhirPathTypeSqlHelper {

  public static final PrimitiveFhirPathTypeSqlHelper INSTANCE =
      new PrimitiveFhirPathTypeSqlHelper();
  
  @Override
  public Column getLiteralColumn(Type litValue) {
    assert PrimitiveType.class.isAssignableFrom(litValue.getClass()) :
      "Encountered non-primitive literal value";
    return lit(((PrimitiveType<?>)litValue).getValue()); 
  }

  @Override
  public BiFunction<Column, Column, Column> getEquality() {
    return Column::equalTo;
  }
  
}
