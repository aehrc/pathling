/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

process.stdin.setEncoding("utf8");

let data = "";

process.stdin.on("readable", () => {
  const chunk = process.stdin.read();
  if (chunk !== null) {
    data += chunk;
  }
});

process.stdin.on("end", () => {
  const bundle = JSON.parse(data);
  const resources = [
    "http://hl7.org/fhir/StructureDefinition/Patient",
    "http://hl7.org/fhir/StructureDefinition/Encounter",
    "http://hl7.org/fhir/StructureDefinition/Condition",
    "http://hl7.org/fhir/StructureDefinition/AllergyIntolerance",
    "http://hl7.org/fhir/StructureDefinition/Observation",
    "http://hl7.org/fhir/StructureDefinition/DiagnosticReport",
    "http://hl7.org/fhir/StructureDefinition/ProcedureRequest",
    "http://hl7.org/fhir/StructureDefinition/ImagingStudy",
    "http://hl7.org/fhir/StructureDefinition/Immunization",
    "http://hl7.org/fhir/StructureDefinition/CarePlan",
    "http://hl7.org/fhir/StructureDefinition/MedicationRequest",
    "http://hl7.org/fhir/StructureDefinition/Claim",
    "http://hl7.org/fhir/StructureDefinition/ExplanationOfBenefit",
    "http://hl7.org/fhir/StructureDefinition/Coverage"
  ];
  bundle["entry"] = bundle["entry"].filter(e =>
    resources.includes(e["resource"]["url"])
  );
  process.stdout.write(JSON.stringify(bundle, null, 2));
});
