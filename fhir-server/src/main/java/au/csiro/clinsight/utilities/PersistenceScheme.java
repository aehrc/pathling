/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;

/**
 * @author John Grimes
 */
public abstract class PersistenceScheme {

  public static String fileNameForResource(String resourceUri) {
    assert resourceUri.startsWith(BASE_RESOURCE_URL_PREFIX)
        : "Attempt to get file name for resource which is not a base resource: " + resourceUri;
    return resourceUri.replaceFirst(BASE_RESOURCE_URL_PREFIX, "") + ".parquet";
  }

}
