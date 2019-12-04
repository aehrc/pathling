/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.clinsight.bunsen.r4.python;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import au.csiro.clinsight.bunsen.FhirEncoders;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Resource;

/**
 * Common functions for working with FHIR resources.
 */
public class Functions {

  private static final FhirContext CONTEXT = FhirContext.forR4();

  /**
   * Converts a set of FHIR resources to JSON.
   *
   * @param dataset a dataset containing FHIR resources
   * @param resourceType the FHIR resource type
   * @return a dataset of JSON strings for the FHIR resources
   */
  public static Dataset<String> toJson(Dataset<?> dataset, String resourceType) {

    Dataset<IBaseResource> resourceDataset =
        dataset.as(FhirEncoders.forR4()
            .getOrCreate()
            .of(resourceType));

    return resourceDataset.map(new ToJson(), Encoders.STRING());
  }

  /**
   * Returns a bundle containing all resources in the dataset. This should be used with caution for
   * large datasets, since the returned bundle will include all data.
   *
   * @param dataset a dataset of FHIR resoruces
   * @return a bundle containing those resources.
   */
  public static Bundle toBundle(Dataset<? extends Resource> dataset) {

    List<Resource> resources = (List<Resource>) dataset.collectAsList();

    Bundle bundle = new Bundle();

    for (Resource resource : resources) {
      bundle.addEntry().setResource(resource);
    }

    return bundle;
  }

  /**
   * Returns a JSON string representing the resources as a bundle. This should be used with caution
   * for large datasets, since the returned string will include all data.
   *
   * @param dataset a dataset of FHIR resources.
   * @return A string containing the JSON representation of the bundle.
   */
  public static String toJsonBundle(Dataset<? extends Resource> dataset) {

    Bundle bundle = toBundle(dataset);

    return CONTEXT.newJsonParser().encodeResourceToString(bundle);
  }

  static class ToJson implements MapFunction<IBaseResource, String> {

    /**
     * Create a new parser instance for each object, since they are not guaranteed to be thread
     * safe.
     */
    private transient IParser parser = CONTEXT.newJsonParser();

    private void readObject(ObjectInputStream in) throws IOException,
        ClassNotFoundException {

      in.defaultReadObject();

      parser = CONTEXT.newJsonParser();
    }

    public String call(IBaseResource resource) throws Exception {

      return parser.encodeResourceToString(resource);
    }
  }

  /**
   * Converts a resource to its XML representation.
   *
   * @param resource the resource
   * @return a string containing XML for the resource.
   */
  public static String resourceToXml(Resource resource) {

    IParser parser = CONTEXT.newXmlParser();

    return parser.encodeResourceToString(resource);
  }

  /**
   * Converts a resource to its JSON representation.
   *
   * @param resource the resource
   * @return a string containing JSON for the resource.
   */
  public static String resourceToJson(Resource resource) {

    IParser parser = CONTEXT.newJsonParser();

    return parser.encodeResourceToString(resource);
  }
}
