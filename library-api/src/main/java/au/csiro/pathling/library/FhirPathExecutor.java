package au.csiro.pathling.library;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.evaluation.EvaluationContext;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public record FhirPathExecutor(DataSource dataSource, EvaluationContext context) {

  public Dataset<Row> execute(final String resourceType, final String fhirPath) {
    final Dataset<Row> data = dataSource.read(resourceType);
    final Parser parser = new Parser();
    final FhirPath parsed = parser.parse(fhirPath);
    final Column column = functions.struct(Stream.of(data.columns())
        .map(functions::col)
        .toArray(Column[]::new));
    final FhirPathType type = new FhirPathType(resourceType);
    final Collection input = new ResourceCollection(column, Optional.of(type));
    final Collection output = parsed.evaluate(input, context);
    return data.select(output.getColumn());
  }

}
