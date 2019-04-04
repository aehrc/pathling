/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import com.cerner.bunsen.Bundles;
import com.cerner.bunsen.Bundles.BundleContainer;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
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
@SuppressWarnings({"WeakerAccess", "unused"})
public class FhirLoader {

  private static final Logger logger = LoggerFactory.getLogger(FhirLoader.class);

  private final FhirLoaderConfiguration configuration;
  private final SparkSession spark;

  public FhirLoader(FhirLoaderConfiguration configuration) {
    logger.debug("Creating new FhirLoader: " + configuration);
    this.configuration = configuration;

    SparkConf sparkConf = new SparkConf();
    sparkConf.set("spark.master", configuration.getSparkMasterUrl());
    sparkConf.set("spark.sql.warehouse.dir", configuration.getWarehouseDirectory());
    sparkConf.set("javax.jdo.option.ConnectionURL", configuration.getMetastoreUrl());
    sparkConf.set("javax.jdo.option.ConnectionUserName", configuration.getMetastoreUser());
    sparkConf.set("javax.jdo.option.ConnectionPassword", configuration.getMetastorePassword());
    sparkConf.set("spark.executor.memory", configuration.getExecutorMemory());

    List<String> sparkJars = new ArrayList<>();
    sparkJars.add("/fhir-loader.jar");
    File libDirectory = new File("/lib");
    File[] dependencyJars = libDirectory.listFiles(file -> file.getName().matches(".*\\.jar"));
    assert dependencyJars != null;
    for (File dependencyJar : dependencyJars) {
      sparkJars.add(dependencyJar.getAbsolutePath());
    }
    sparkConf.set("spark.jars", String.join(",", sparkJars));

    logger.debug("Spark configuration: " + sparkConf.toDebugString());
    spark = SparkSession.builder()
        .appName("fhir-loader")
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate();
  }

  /**
   * Loads the content of each JSON Bundle file matched by a supplied glob.
   */
  public void processJsonBundles(String bundlesPath) {
    logger.info("Processing JSON Bundles at: " + bundlesPath);

    Bundles bundles = Bundles.forStu3();
    JavaRDD<BundleContainer> rdd = bundles
        .loadFromDirectory(spark, bundlesPath, configuration.getLoadPartitions());
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
  public void processNdjsonFile(URL ndjsonFile) {
    // TODO: Implement loading of NDJSON files.
    throw new RuntimeException("Not yet implemented");
  }
}
