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

/**
 * Takes either a FHIR transaction or an NDJSON file as input, and updates the target data mart with
 * the information within the FHIR resource(s).
 *
 * @author John Grimes
 */
public class FhirLoader {

  private FhirLoaderConfiguration configuration;
  private SparkSession sparkSession;

  public FhirLoader(FhirLoaderConfiguration configuration) {
    checkArgument(configuration.getSparkMasterUrl() != null, "Must supply Spark master URL");
    checkArgument(configuration.getWarehouseDirectory() != null, "Must supply warehouse directory");
    checkArgument(configuration.getMetastoreConnectionUrl() != null,
        "Must supply metastore connection URL");
    checkArgument(configuration.getMetastoreUser() != null, "Must supply metastore user");
    checkArgument(configuration.getMetastorePassword() != null, "Must supply metastore password");

    this.configuration = configuration;

    sparkSession = SparkSession.builder()
        .config("spark.master", configuration.getSparkMasterUrl())
        // TODO: Use Maven dependency plugin to copy this into a relative location.
        .config("spark.jars",
            "/Users/gri306/Code/contrib/bunsen/bunsen-shaded/target/bunsen-shaded-0.4.6-SNAPSHOT.jar")
        .config("spark.sql.warehouse.dir", configuration.getWarehouseDirectory())
        .config("javax.jdo.option.ConnectionURL", configuration.getMetastoreConnectionUrl())
        .config("javax.jdo.option.ConnectionUserName", configuration.getMetastoreUser())
        .config("javax.jdo.option.ConnectionPassword", configuration.getMetastorePassword())
        .enableHiveSupport()
        .getOrCreate();
  }

  /**
   * Loads the content of each JSON Bundle file matched by a supplied glob.
   */
  public void processJsonBundles(File bundlesDirectory) {
    checkArgument(bundlesDirectory.isDirectory(), "bundlesDirectory must be a directory");
    Bundles bundles = Bundles.forStu3();
    JavaRDD<BundleContainer> rdd = bundles
        .loadFromDirectory(sparkSession, bundlesDirectory.getAbsolutePath(),
            configuration.getLoadPartitions());
    sparkSession.sql("CREATE DATABASE IF NOT EXISTS " + configuration.getDatabaseName());
    for (String resourceName : configuration.getResourcesToSave()) {
      Dataset ds = bundles.extractEntry(sparkSession, rdd, resourceName);
      ds.write().mode(SaveMode.Overwrite)
          .saveAsTable(configuration.getDatabaseName() + "." + resourceName.toLowerCase());
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
