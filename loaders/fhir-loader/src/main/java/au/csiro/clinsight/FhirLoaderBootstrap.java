/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;

/**
 * @author John Grimes
 */
public class FhirLoaderBootstrap {

  public static void main(String[] args) throws Exception {
    String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
    String jsonBundlesDirectory = System.getenv("JSON_BUNDLES_DIRECTORY");

    checkArgument(sparkMasterUrl != null, "Must supply sparkMasterUrl property");
    checkArgument(jsonBundlesDirectory != null, "Must supply jsonBundlesDirectory property");

    FhirLoader fhirLoader = new FhirLoader(sparkMasterUrl);
    fhirLoader.processJsonBundles(new File(jsonBundlesDirectory));
  }

}
