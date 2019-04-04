/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static au.csiro.clinsight.utilities.Configuration.setStringPropsUsingEnvVar;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author John Grimes
 */
public class FhirLoaderBootstrap {

  public static void main(String[] args) {
    FhirLoaderConfiguration config = new FhirLoaderConfiguration();

    setStringPropsUsingEnvVar(config, new HashMap<String, String>() {{
      put("CLINSIGHT_SPARK_MASTER_URL", "sparkMasterUrl");
      put("CLINSIGHT_WAREHOUSE_DIRECTORY", "warehouseDirectory");
      put("CLINSIGHT_METASTORE_URL", "metastoreUrl");
      put("CLINSIGHT_METASTORE_USER", "metastoreUser");
      put("CLINSIGHT_METASTORE_PASSWORD", "metastorePassword");
      put("CLINSIGHT_DATABASE_NAME", "databaseName");
      put("CLINSIGHT_EXECUTOR_MEMORY", "executorMemory");
    }});
    String loadPartitions = System.getenv("CLINSIGHT_LOAD_PARTITIONS");
    if (loadPartitions != null) {
      config.setLoadPartitions(Integer.parseInt(loadPartitions));
    }

    // This list is based upon the resources produced by Synthea, and will need to be expanded if
    // loading FHIR data which covers additional resources.
    List<String> resourcesToSave = Arrays.asList("Patient",
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
        "Coverage");
    config.getResourcesToSave().addAll(resourcesToSave);

    String jsonBundlesPath = System.getenv("CLINSIGHT_JSON_BUNDLES_PATH");
    assert jsonBundlesPath != null : "Must supply CLINSIGHT_JSON_BUNDLES_PATH parameter";

    FhirLoader fhirLoader = new FhirLoader(config);
    fhirLoader.processJsonBundles(jsonBundlesPath);
  }

}
