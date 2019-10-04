package au.csiro.clinsight.fhir.definitions;

import static org.assertj.core.api.Assertions.assertThat;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hl7.fhir.r4.model.ElementDefinition;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ResourceScannerTest {

  IParser jsonParser;

  @Before
  public void setUp() throws Exception {
    jsonParser = FhirContext.forR4().newJsonParser();
  }

  @Test
  public void summariseResourceDefinitions() {
    // Get the actual ElementDefinition from the StructureDefinition, for later comparison with the
    // output of the summariseDefinitions method.
    InputStream encounterStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("fhir/Encounter.StructureDefinition.json");
    assertThat(encounterStream).isNotNull();
    StructureDefinition encounter = (StructureDefinition) jsonParser.parseResource(encounterStream);

    Optional<ElementDefinition> optionalSubject = encounter.getSnapshot().getElement().stream()
        .filter(element -> element.getPath().equals("Encounter.subject")).findFirst();
    assertThat(optionalSubject.isPresent()).isTrue();
    ElementDefinition subject = optionalSubject.get();

    // Execute the method.
    Map<ResourceType, Map<String, au.csiro.clinsight.fhir.definitions.ElementDefinition>> definitions =
        ResourceScanner.summariseResourceDefinitions(Collections.singletonList(encounter));

    // Check the result.
    Map<String, au.csiro.clinsight.fhir.definitions.ElementDefinition> elementDefinitionMap =
        definitions.get(ResourceType.ENCOUNTER);
    au.csiro.clinsight.fhir.definitions.ElementDefinition elementDefinition = elementDefinitionMap
        .get("Encounter.subject");
    assertThat(elementDefinition.getChildElements()).isEmpty();
    assertThat(elementDefinition.getReferenceTypes())
        .containsOnly(ResourceType.PATIENT, ResourceType.GROUP);
    assertThat(elementDefinition.getPath()).isEqualTo("Encounter.subject");
    assertThat(elementDefinition.getFhirType()).isEqualTo(FHIRDefinedType.REFERENCE);
    assertThat(elementDefinition.getMaxCardinality()).isEqualTo("1");
  }
}