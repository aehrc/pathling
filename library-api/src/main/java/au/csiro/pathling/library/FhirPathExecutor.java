package au.csiro.pathling.library;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public record FhirPathExecutor(DataSource dataSource) {

  public Dataset<Row> execute(final String resourceType, final String fhirPath) {
    final Dataset<Row> data = dataSource.read(resourceType);
    final Parser parser = new Parser();
    final FhirPath parsed = parser.parse(fhirPath);
    return parsed.evaluate(input, context);
  }

}
