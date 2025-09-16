package au.csiro.pathling.util;

import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.SparkListener;
import au.csiro.pathling.async.StageMap;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.stubs.TestTerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

/**
 * @author Felix Naumann
 */
@TestConfiguration
public class FhirServerTestConfiguration {

  @Primary
  @Bean
  public JobRegistry jobRegistry() {
    return new JobRegistry();
  }
  
  @Primary
  @Bean
  public QueryableDataSource deltaLake(PathlingContext pathlingContext) {
    return new DataSourceBuilder(pathlingContext).delta(
        Path.of("src/test/resources/test-data/fhir/delta").toAbsolutePath().toString());
  }

  @Bean
  @ConditionalOnMissingBean
  @Primary
  @Nonnull
  static SparkSession sparkSession(
      @Nonnull final Environment environment,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory
      /*@Nonnull final Optional<SparkListener> sparkListener*/) {
    // TODO: See it this properies can be set from Environment (extract common code from Spark class)
    final SparkSession spark = SparkSession.builder()
        .master("local[*]")
        .appName("pathling-unittest")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir",
            "file://" + Path.of("src/test/resources/test-data/fhir/out").toAbsolutePath()
                .toString())
        .getOrCreate();
    //TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    //FhirpathUDFRegistrar.registerUDFs(spark);
    return spark;
  }

  @Primary
  @Bean
  public PathlingContext pathlingContext(SparkSession sparkSession) {
    return PathlingContext.create(sparkSession);
  }

  @Bean
  @ConditionalOnMissingBean
  @Nonnull
  static TerminologyServiceFactory terminologyClientFactory() {
    return new TestTerminologyServiceFactory();
  }

  @Bean
  public SparkListener sparkListener(JobRegistry jobRegistry, StageMap stageMap,
      SparkSession sparkSession) {
    return new SparkListener(jobRegistry, stageMap, sparkSession);
  }

  @Primary
  @Bean
  public StageMap stageMap() {
    return new StageMap();
  }
  
  @Primary
  @Bean
  public TestDataSetup testDataSetup(PathlingContext pathlingContext) {
    return new TestDataSetup(pathlingContext);
  }
} 
