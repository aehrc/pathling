package au.csiro.pathling.fhirpath.definition;

import static au.csiro.pathling.utilities.Functions.maybeCast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@Slf4j
class NodeDefinitionTest {

  @Autowired
  FhirContext fhirContext;


  @Test
    // test that choice with reference type returns the correct type of definition
  void testSimpleReference() {

    final ResourceDefinition conditionDefinition = new ResourceDefinition(
        ResourceType.CONDITION,
        fhirContext.getResourceDefinition(ResourceType.CONDITION.toCode()));

    final ChildDefinition referenceDefinition = conditionDefinition.getChildElement("subject")
        .orElseThrow();
    assertInstanceOf(ReferenceDefinition.class, referenceDefinition);
  }


  @Test
    // test that choice with reference type returns the correct type of definition
  void testReferenceInChoiceValue() {

    final ResourceDefinition observationDefinition = new ResourceDefinition(
        ResourceType.MEDICATIONDISPENSE,
        fhirContext.getResourceDefinition(ResourceType.MEDICATIONDISPENSE.toCode()));

    final ChildDefinition medicationValue = observationDefinition.getChildElement("medication")
        .orElseThrow();
    assertInstanceOf(ChoiceChildDefinition.class, medicationValue);
    final ElementDefinition referenceDefinition = ((ChoiceChildDefinition) medicationValue).getChildByType(
            "Reference")
        .orElseThrow();
    assertInstanceOf(ReferenceDefinition.class, referenceDefinition);
    assertEquals(Set.of(ResourceType.MEDICATION),
        ((ReferenceDefinition) referenceDefinition).getReferenceTypes());
    assertEquals("medicationReference", referenceDefinition.getElementName());
  }

  @Test
  void testReferenceInExtensionValue() {
    // test that choice with reference type returns the correct type of definition in extension

    final ResourceDefinition patientDefinition = new ResourceDefinition(ResourceType.PATIENT,
        fhirContext.getResourceDefinition(Patient.class));

    // test that choice with reference type returns the correct type of definition in extensionp
    final ElementDefinition referenceDefinition = patientDefinition.getChildElement("extension")
        .flatMap(extension -> extension.getChildElement("value"))
        .flatMap(maybeCast(ChoiceChildDefinition.class))
        .flatMap(value -> value.getChildByType("Reference"))
        .orElseThrow();
    assertInstanceOf(ReferenceDefinition.class, referenceDefinition);
    assertEquals("valueReference", referenceDefinition.getElementName());
    System.out.println(((ReferenceDefinition) referenceDefinition).getReferenceTypes());
  }
}
