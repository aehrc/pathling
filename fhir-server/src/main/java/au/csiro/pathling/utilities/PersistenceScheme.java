/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;

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
