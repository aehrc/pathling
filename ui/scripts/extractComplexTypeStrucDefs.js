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
    "http://hl7.org/fhir/StructureDefinition/Ratio",
    "http://hl7.org/fhir/StructureDefinition/Period",
    "http://hl7.org/fhir/StructureDefinition/Range",
    "http://hl7.org/fhir/StructureDefinition/Attachment",
    "http://hl7.org/fhir/StructureDefinition/Identifier",
    "http://hl7.org/fhir/StructureDefinition/HumanName",
    "http://hl7.org/fhir/StructureDefinition/Annotation",
    "http://hl7.org/fhir/StructureDefinition/Address",
    "http://hl7.org/fhir/StructureDefinition/ContactPoint",
    "http://hl7.org/fhir/StructureDefinition/SampledData",
    "http://hl7.org/fhir/StructureDefinition/Money",
    "http://hl7.org/fhir/StructureDefinition/Count",
    "http://hl7.org/fhir/StructureDefinition/Duration",
    "http://hl7.org/fhir/StructureDefinition/SimpleQuantity",
    "http://hl7.org/fhir/StructureDefinition/Quantity",
    "http://hl7.org/fhir/StructureDefinition/Distance",
    "http://hl7.org/fhir/StructureDefinition/Age",
    "http://hl7.org/fhir/StructureDefinition/CodeableConcept",
    "http://hl7.org/fhir/StructureDefinition/Signature",
    "http://hl7.org/fhir/StructureDefinition/Coding",
    "http://hl7.org/fhir/StructureDefinition/Timing",
    "http://hl7.org/fhir/StructureDefinition/Reference"
  ];
  bundle["entry"] = bundle["entry"].filter(e =>
    resources.includes(e["resource"]["url"])
  );
  process.stdout.write(JSON.stringify(bundle, null, 2));
});
