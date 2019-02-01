/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static com.google.common.base.Preconditions.checkArgument;

import com.cerner.bunsen.Bundles;
import com.cerner.bunsen.Bundles.BundleContainer;
import java.io.File;
import java.net.URL;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes either a FHIR transaction or an NDJSON file as input, and updates the target data mart with
 * the information within the FHIR resource(s).
 *
 * @author John Grimes
 */
public class FhirLoader {

  private static Logger logger = LoggerFactory.getLogger(FhirLoader.class);

  private FhirLoaderConfiguration configuration;
  private SparkSession spark;

  public FhirLoader(FhirLoaderConfiguration configuration) {
    checkArgument(configuration.getSparkMasterUrl() != null, "Must supply Spark master URL");
    checkArgument(configuration.getWarehouseDirectory() != null, "Must supply warehouse directory");
    checkArgument(configuration.getMetastoreUrl() != null,
        "Must supply metastore connection URL");
    checkArgument(configuration.getMetastoreUser() != null, "Must supply metastore user");
    checkArgument(configuration.getMetastorePassword() != null, "Must supply metastore password");
    checkArgument(configuration.getExecutorMemory() != null, "Must supply executor memory");

    logger.debug("Creating new FhirLoader: " + configuration);
    this.configuration = configuration;

    spark = SparkSession.builder()
        .config("spark.master", configuration.getSparkMasterUrl())
        // TODO: Use Maven dependency plugin to copy this into a relative location.
        .config("spark.jars",
            "/Users/gri306/Code/contrib/bunsen/bunsen-shaded/target/bunsen-shaded-0.4.6-SNAPSHOT.jar")
        .config("spark.sql.warehouse.dir", configuration.getWarehouseDirectory())
        .config("javax.jdo.option.ConnectionURL", configuration.getMetastoreUrl())
        .config("javax.jdo.option.ConnectionUserName", configuration.getMetastoreUser())
        .config("javax.jdo.option.ConnectionPassword", configuration.getMetastorePassword())
        .config("spark.executor.memory", configuration.getExecutorMemory())
        .enableHiveSupport()
        .getOrCreate();
  }

  /**
   * Loads the content of each JSON Bundle file matched by a supplied glob.
   */
  public void processJsonBundles(File bundlesDirectory) {
    checkArgument(bundlesDirectory.isDirectory(), "bundlesDirectory must be a directory");
    logger.info("Processing JSON Bundles at: " + bundlesDirectory.getAbsolutePath());

    Bundles bundles = Bundles.forStu3();
    JavaRDD<BundleContainer> rdd = bundles
        .loadFromDirectory(spark, bundlesDirectory.getAbsolutePath(),
            configuration.getLoadPartitions());
    spark.sql("CREATE DATABASE IF NOT EXISTS " + configuration.getDatabaseName());
    for (String resourceName : configuration.getResourcesToSave()) {
      String tableName = configuration.getDatabaseName() + "." + resourceName.toLowerCase();
      logger.info("Saving table: " + tableName);

      Dataset ds = bundles.extractEntry(spark, rdd, resourceName);
      ds.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
    }
  }

  /**
   * Loads each transaction (Bundle) within an NDJSON file, accessible from the supplied URL.
   */
  public void processNdjsonFile(URL ndjsonFile) throws Exception {
    // TODO: Implement loading of NDJSON files.
    throw new RuntimeException("Not yet implemented");
  }
}
