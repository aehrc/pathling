package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.parser.ParserContext;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Value
public class FhirPathContextAndResult {

  @Nonnull
  FhirPath fhirPath;

  @Nonnull
  ParserContext context;

  @Nonnull
  Dataset<Row> result;
  
}
