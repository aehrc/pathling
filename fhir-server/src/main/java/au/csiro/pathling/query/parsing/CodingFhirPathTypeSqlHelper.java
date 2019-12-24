package au.csiro.pathling.query.parsing;

import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Type;
import static org.apache.spark.sql.functions.*;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public class CodingFhirPathTypeSqlHelper implements FhirPathTypeSqlHelper {


  public static final CodingFhirPathTypeSqlHelper INSTANCE = new CodingFhirPathTypeSqlHelper();

  private static final List<String> COLUMNS_TO_COMPARE = Arrays.asList("system", "version", "code");

  @Override
  public Column getLiteralColumn(Type litValue) {
    assert Coding.class
        .isAssignableFrom(litValue.getClass()) : "Encountered non Coding literal value";
    Coding coding = (Coding) litValue;
    return struct(lit(coding.getId()).alias("id"), lit(coding.getSystem()).alias("system"),
        lit(coding.getVersion()).alias("version"), lit(coding.getCode()).alias("code"),
        lit(coding.getDisplay()).alias("display"), lit(coding.getUserSelected()).alias("userSelected"));
  }

  @Override
  public BiFunction<Column, Column, Column> getEquality() {
    return EqualityUtils.structEqual(COLUMNS_TO_COMPARE);
  }

}
