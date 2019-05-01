/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

/* START Core FHIR types */

export interface Coding {
  system?: string;
  version?: string;
  code?: string;
  display?: string;
  userSelected?: boolean;
}

export interface CodeableConcept {
  coding: Coding[];
  text?: string;
}

export interface OperationOutcomeIssue {
  severity: "fatal" | "error" | "warning" | "information";
  code: string;
  details?: CodeableConcept;
  diagnostics?: string;
  location?: string[];
  expression?: string[];
}

export interface OperationOutcome extends Resource {
  issue: OperationOutcomeIssue[];
}

export interface Parameter {
  name: string;
  resource?: Resource;
  part?: Parameter[];
  [value: string]: any;
}

export interface Parameters extends Resource {
  parameter: Parameter[];
}

interface Resource {
  resourceType: string;
}

/* END Core FHIR types */

/* START Aggregate Query operation types */

export interface AggregationRequestParameter extends Parameter {
  name: string;
  part: StringParameter[];
}

export interface GroupingRequestParameter extends Parameter {
  name: string;
  part: StringParameter[];
}

export interface StringParameter {
  name: string;
  valueString: string;
}

/* END Aggregate Query operation types */
