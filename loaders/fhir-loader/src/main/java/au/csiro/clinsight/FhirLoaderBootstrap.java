/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;

/**
 * @author John Grimes
 */
public class FhirLoaderBootstrap {

  public static void main(String[] args) {
    String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
    String jsonBundlesDirectory = System.getenv("JSON_BUNDLES_DIRECTORY");
    String[] resourcesToSave = {"Patient",
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

    checkArgument(sparkMasterUrl != null, "Must supply sparkMasterUrl property");
    checkArgument(jsonBundlesDirectory != null, "Must supply jsonBundlesDirectory property");

    FhirLoaderConfiguration configuration = new FhirLoaderConfiguration();
    configuration.setSparkMasterUrl(sparkMasterUrl);
    configuration.setWarehouseDirectory("/Users/gri306/Code/clinsight/clinsight/spark-warehouse");
    configuration.setMetastoreUrl("jdbc:postgresql://localhost/clinsight_metastore");
    configuration.setMetastoreUser("gri306");
    configuration.setMetastorePassword("");
    configuration.setLoadPartitions(12);
    configuration.setDatabaseName("clinsight");
    configuration.setResourcesToSave(resourcesToSave);
    configuration.setExecutorMemory("6g");

    FhirLoader fhirLoader = new FhirLoader(configuration);
    fhirLoader.processJsonBundles(new File(jsonBundlesDirectory));
  }

}
