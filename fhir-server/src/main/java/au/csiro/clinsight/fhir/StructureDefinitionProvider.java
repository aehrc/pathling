/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.StructureDefinition;

/**
 * This class is used for serving up the StructureDefinition resources which describe this server's
 * FHIR API.
 *
 * @author John Grimes
 */
public class StructureDefinitionProvider implements IResourceProvider {

  private final IParser jsonParser;
  private static final Map<String, String> idToResourcePath = new HashMap<String, String>() {{
    put("StructureDefinition/available-resource-type-0",
        "fhir/AvailableResourceType.StructureDefinition.json");
    put("StructureDefinition/fhir-server-capabilities-0",
        "fhir/FhirServerCapabilities.StructureDefinition.json");
  }};
  private static final Map<String, StructureDefinition> resourceCache = new HashMap<>();

  public StructureDefinitionProvider(FhirContext fhirContext) {
    jsonParser = fhirContext.newJsonParser();
  }

  @Override
  public Class<? extends IBaseResource> getResourceType() {
    return StructureDefinition.class;
  }

  @Read()
  public StructureDefinition getStructureDefinitionById(@IdParam IdType id) {
    return getResource(id.getValueAsString());
  }

  private StructureDefinition getResource(String id) {
    StructureDefinition cached = resourceCache.get(id);
    if (cached != null) {
      return cached;
    }
    String resourcePath = idToResourcePath.get(id);
    if (resourcePath == null) {
      throw new ResourceNotFoundException("StructureDefinition not found: " + id);
    }
    InputStream resourceStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(resourcePath);
    return (StructureDefinition) jsonParser.parseResource(resourceStream);
  }

}
