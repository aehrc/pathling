package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat;
import org.apache.spark.sql.execution.datasources.v2.json.JsonDataSourceV2;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Some;
import scala.collection.Seq;

public class FhirJsonProvider extends JsonDataSourceV2 {

  @Override
  @Nonnull
  public String shortName() {
    return "fhir+json";
  }

  @Override
  @Nonnull
  public Table getTable(@Nullable final CaseInsensitiveStringMap options) {
    final Seq<String> paths = getPaths(options);
    final String tableName = getTableName(options, paths);
    final CaseInsensitiveStringMap optionsWithoutPaths = getOptionsWithoutPaths(options);
    return new FhirJsonTable(getFhirContext(), tableName, sparkSession(), optionsWithoutPaths,
        paths, Option.empty(), JsonFileFormat.class);
  }

  @Override
  @Nonnull
  public Table getTable(final CaseInsensitiveStringMap options, final StructType schema) {
    final Seq<String> paths = getPaths(options);
    final String tableName = getTableName(options, paths);
    final CaseInsensitiveStringMap optionsWithoutPaths = getOptionsWithoutPaths(options);
    return new FhirJsonTable(getFhirContext(), tableName, sparkSession(), optionsWithoutPaths,
        paths, new Some<>(schema), JsonFileFormat.class);
  }

  private static FhirContext getFhirContext() {
    return FhirContext.forR4Cached();
  }

}
