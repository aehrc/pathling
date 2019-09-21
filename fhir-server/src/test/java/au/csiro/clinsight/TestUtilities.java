/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.TerminologyClient;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.collect.Sets;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
public abstract class TestUtilities {

  private static final FhirContext fhirContext = FhirContext.forR4();
  private static final IParser jsonParser = fhirContext.newJsonParser();

  private static final InputStream definitionsStream = Thread.currentThread()
      .getContextClassLoader()
      .getResourceAsStream("fhir/fhir-definitions.Bundle.json");

  private static final Bundle definitionsBundle = (Bundle) jsonParser
      .parseResource(definitionsStream);

  private static final List<StructureDefinition> profiles = definitionsBundle
      .getEntry()
      .stream()
      .filter(entry -> entry.getResource().getResourceType() == ResourceType.StructureDefinition)
      .map(entry -> (StructureDefinition) entry.getResource())
      .collect(Collectors.toList());

  static void mockDefinitionRetrieval(TerminologyClient terminologyClient) {
    when(terminologyClient.getAllStructureDefinitions(Sets.newHashSet("url", "kind")))
        .thenReturn(profiles);
    when(terminologyClient.getStructureDefinitionById(any(IdType.class))).thenAnswer(
        (Answer<StructureDefinition>) invocation -> {
          IdType theId = invocation.getArgument(0);
          return profiles.stream()
              .filter(sd -> sd.getId().equals(theId.asStringValue()))
              .findFirst()
              .orElse(null);
        }
    );
  }

}
