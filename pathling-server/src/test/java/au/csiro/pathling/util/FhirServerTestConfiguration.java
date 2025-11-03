package au.csiro.pathling.util;

import au.csiro.pathling.async.JobProvider;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.async.SparkJobListener;
import au.csiro.pathling.async.StageMap;
import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.export.ExportResultRegistry;
import au.csiro.pathling.sql.udf.TerminologyUdfRegistrar;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.stubs.TestTerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author Felix Naumann
 */
@TestConfiguration
@EnableConfigurationProperties
public class FhirServerTestConfiguration {

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public JobRegistry jobRegistry() {
    return new JobRegistry();
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public QueryableDataSource deltaLake(PathlingContext pathlingContext) {
    return new DataSourceBuilder(pathlingContext).delta(
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath().toString());
  }

  @Bean
  @ConditionalOnMissingBean
  @Primary
  @Nonnull
  static SparkSession sparkSession(
      @Nonnull final Environment environment,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory
      /*@Nonnull final Optional<SparkListener> sparkListener*/) {
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
    TerminologyUdfRegistrar.registerUdfs(spark, terminologyServiceFactory);
    //FhirpathUDFRegistrar.registerUDFs(spark);
    return spark;
  }

  @Primary
  @ConditionalOnMissingBean
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
  public SparkJobListener sparkListener(JobRegistry jobRegistry, StageMap stageMap,
      SparkSession sparkSession) {
    return new SparkJobListener(jobRegistry, stageMap, sparkSession);
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public StageMap stageMap() {
    return new StageMap();
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public TestDataSetup testDataSetup(PathlingContext pathlingContext) {
    return new TestDataSetup(pathlingContext);
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean(destroyMethod = "shutdown")
  public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.initialize();
    return executor;
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public CacheableDatabase cacheableDatabase(SparkSession sparkSession,
      @Value("${pathling.storage.warehouseUrl}") String warehouseUrl,
      ThreadPoolTaskExecutor threadPoolTaskExecutor) {
    return new CacheableDatabase(sparkSession, warehouseUrl, threadPoolTaskExecutor);
  }

  @Primary
  @ConditionalOnMissingBean
  @Bean
  public RequestTagFactory requestTagFactory(ServerConfiguration serverConfiguration,
      CacheableDatabase cacheableDatabase) {
    return new RequestTagFactory(cacheableDatabase, serverConfiguration);
  }

  @Bean
  @ConditionalOnMissingBean
  @Primary
  public JobProvider jobProvider(
      ServerConfiguration serverConfiguration,
      JobRegistry jobRegistry,
      SparkSession sparkSession,
      @Value("${pathling.storage.warehouseUrl}") String warehouseUrl
  ) {
    return new JobProvider(
        serverConfiguration,
        jobRegistry,
        sparkSession,
        warehouseUrl
    );
  }

  @Bean
  @Primary
  @ConditionalOnMissingBean
  @ConfigurationProperties("pathling")
  public ServerConfiguration serverConfiguration() {
    return new ServerConfiguration();
  }

  @Bean
  @Primary
  @ConditionalOnMissingBean
  public ExportResultRegistry exportResultRegistry() {
    return new ExportResultRegistry();
  }
} 
