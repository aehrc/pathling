package au.csiro.pathling.benchmark;

import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.library.query.QueryDispatcher;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.source.NdjsonSource;
import au.csiro.pathling.library.io.source.ParquetSource;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import com.google.gson.Gson;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@State(Scope.Benchmark)
public class PathlingBenchmark {

  @Param({"ndjson", "parquet"})
  public String dataSourceType;

  @Param({"simple", "nested", "complex"})
  public String viewDefName;

  private SparkSession spark;
  private PathlingContext context;
  private DataSource dataSource;
  private QueryDispatcher dispatcher;
  private String viewJson;
  private Gson gson;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    spark = SparkSession.builder().appName("PathlingBenchmark").master("local[*]").getOrCreate();
    context = PathlingContext.create(spark);
    gson = context.getGson();
    String baseDir = System.getProperty("benchmark.dataDir", "data");
    if (dataSourceType.equals("ndjson")) {
      dataSource = new NdjsonSource(context, baseDir + "/ndjson");
    } else {
      dataSource = new ParquetSource(context, baseDir + "/parquet");
    }
    dispatcher = new QueryDispatcher(new FhirViewExecutor(context.getFhirContext(), spark, dataSource));
    try (InputStream in = getClass().getResourceAsStream("/" + viewDefName + ".json")) {
      byte[] bytes = in.readAllBytes();
      viewJson = new String(bytes, StandardCharsets.UTF_8);
    }
  }

  @Benchmark
  public long runView() {
    ResourceType resource = ResourceType.Patient;
    return new FhirViewQuery(dispatcher, resource, gson)
        .json(viewJson)
        .execute()
        .count();
  }

  @TearDown(Level.Trial)
  public void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }
}

