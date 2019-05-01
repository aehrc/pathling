/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import rawResourceTree from "../../config/resource-tree.json";
import rawComplexTypesTree from "../../config/complex-type-tree.json";
import rawReverseReferences from "../../config/reverse-references.json";

interface ResourceTree {
  [resourceName: string]: ResourceNode;
}

interface ResourceNode {
  definition: string;
  children: ElementNode[];
}

export interface ElementNode {
  name: string;
  path: string;
  type: string;
  definition: string;
  referenceTypes?: string[];
  children?: ElementNode[];
}

interface ReverseReferences {
  [resourceName: string]: ElementNode[];
}

export const resourceTree: ResourceTree = rawResourceTree;
export const complexTypesTree: ResourceTree = rawComplexTypesTree;
export const reverseReferences: ReverseReferences = rawReverseReferences;

// Reference is supported, but is left out of this list as it is treated
// differently.
export const supportedComplexTypes = [
  "Ratio",
  "Period",
  "Range",
  "Attachment",
  "Identifier",
  "HumanName",
  "Annotation",
  "Address",
  "ContactPoint",
  "SampledData",
  "Money",
  "Count",
  "Duration",
  "SimpleQuantity",
  "Quantity",
  "Distance",
  "Age",
  "CodeableConcept",
  "Signature",
  "Coding",
  "Timing"
];
