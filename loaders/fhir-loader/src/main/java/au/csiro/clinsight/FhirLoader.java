/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import static com.google.common.base.Preconditions.checkArgument;

import com.cerner.bunsen.Bundles;
import com.cerner.bunsen.Bundles.BundleContainer;
import java.io.File;
import java.net.URL;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

/**
 * Takes either a FHIR transaction or an NDJSON file as input, and updates the target data mart with
 * the information within the FHIR resource(s).
 *
 * @author John Grimes
 */
public class FhirLoader {

  private SparkSession sparkSession;
  private int loadPartitions = 12;
  private String databaseName = "clinsight";
  private String[] resourcesToSave = {"Patient",
      "Encounter",
      "Condition",
      "AllergyIntolerance",
      "Observation",
      "DiagnosticReport",
      "ProcedureRequest",
      "ImagingStudy",
      "Immunization",
      "CarePlan",
      "MedicationRequest",
      "Claim",
      "ExplanationOfBenefit",
      "Coverage"};

  public FhirLoader(String sparkMasterUrl) {
    sparkSession = SparkSession.builder()
        .config("spark.master", sparkMasterUrl)
        // TODO: Use Maven dependency plugin to copy this into a relative location.
        .config("spark.jars",
            "/Users/gri306/Code/contrib/bunsen/bunsen-shaded/target/bunsen-shaded-0.4.6-SNAPSHOT.jar")
        .enableHiveSupport()
        .getOrCreate();
  }

  /**
   * Loads the content of each JSON Bundle file matched by a supplied glob.
   */
  public void processJsonBundles(File bundlesDirectory) throws Exception {
    checkArgument(bundlesDirectory.isDirectory(), "bundlesDirectory must be a directory");
    Bundles bundles = Bundles.forStu3();
    JavaRDD<BundleContainer> rdd = bundles
        .loadFromDirectory(sparkSession, bundlesDirectory.getAbsolutePath(), loadPartitions);
    bundles.saveAsDatabase(sparkSession, rdd, databaseName, resourcesToSave);
  }

  /**
   * Loads each transaction (Bundle) within an NDJSON file, accessible from the supplied URL.
   */
  public void processNdjsonFile(URL ndjsonFile) throws Exception {
    // TODO: Implement loading of NDJSON files.
    throw new RuntimeException("Not yet implemented");
  }

  public int getLoadPartitions() {
    return loadPartitions;
  }

  public void setLoadPartitions(int loadPartitions) {
    this.loadPartitions = loadPartitions;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String[] getResourcesToSave() {
    return resourcesToSave;
  }

  public void setResourcesToSave(String[] resourcesToSave) {
    this.resourcesToSave = resourcesToSave;
  }
}
