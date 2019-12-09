/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.utilities;

import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * @author John Grimes
 */
public abstract class PersistenceScheme {

  public static String fileNameForResource(ResourceType resourceType) {
    return resourceType.toCode() + ".parquet";
  }

  public static String convertS3ToS3aUrl(String s3Url) {
    return s3Url.replaceFirst("s3:", "s3a:");
  }

}
