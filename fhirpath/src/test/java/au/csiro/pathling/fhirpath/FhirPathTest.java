package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.Parser;
import ca.uhn.fhir.context.FhirContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class FhirPathTest {

  @Test
  void test() {
    final SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .getOrCreate();
    final FhirContext fhirContext = FhirContext.forR4Cached();
    final Parser parser = new Parser();
    final FhirPath result = parser.parse("name.where(use = 'official').first()");
  }

}
