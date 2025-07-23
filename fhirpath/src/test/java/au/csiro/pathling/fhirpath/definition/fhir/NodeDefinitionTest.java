package au.csiro.pathling.fhirpath.definition.fhir;

import static au.csiro.pathling.utilities.Functions.maybeCast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ElementDefinition;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.context.FhirContext;
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
  void testSimpleReference() {
    // Test that choice with reference type returns the correct type of definition.
    final FhirResourceDefinition conditionDefinition = new FhirResourceDefinition(
        ResourceType.CONDITION,
        fhirContext.getResourceDefinition(ResourceType.CONDITION.toCode()));

    final ChildDefinition referenceDefinition = conditionDefinition.getChildElement("subject")
        .orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
  }


  @Test
  void testReferenceInChoiceValue() {
    // Test that choice with reference type returns the correct type of definition.
    final FhirResourceDefinition observationDefinition = new FhirResourceDefinition(
        ResourceType.MEDICATIONDISPENSE,
        fhirContext.getResourceDefinition(ResourceType.MEDICATIONDISPENSE.toCode()));

    final ChildDefinition medicationValue = observationDefinition.getChildElement("medication")
        .orElseThrow();
    assertInstanceOf(FhirChoiceDefinition.class, medicationValue);
    final ElementDefinition referenceDefinition = ((FhirChoiceDefinition) medicationValue).getChildByType(
            "Reference")
        .orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
    assertEquals("medicationReference", referenceDefinition.getElementName());
  }

  @Test
  void testReferenceInExtensionValue() {
    // Test that choice with reference type returns the correct type of definition in extension.
    final FhirResourceDefinition patientDefinition = new FhirResourceDefinition(
        ResourceType.PATIENT,
        fhirContext.getResourceDefinition(Patient.class));

    // Test that choice with reference type returns the correct type of definition in extension.
    final ElementDefinition referenceDefinition = patientDefinition.getChildElement("extension")
        .flatMap(extension -> extension.getChildElement("value"))
        .flatMap(maybeCast(FhirChoiceDefinition.class))
        .flatMap(value -> value.getChildByType("Reference"))
        .orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
    assertEquals("valueReference", referenceDefinition.getElementName());
  }
}
