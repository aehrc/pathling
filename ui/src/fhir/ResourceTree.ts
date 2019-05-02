/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import rawResourceTree from "../../config/resource-tree.json";
import rawComplexTypesTree from "../../config/complex-type-tree.json";
import rawReverseReferences from "../../config/reverse-references.json";

interface ResourceTree {
  [resourceName: string]: ResourceNode;
}

export interface ResourceNode {
  definition: string;
  contains: ElementNode[];
}

export interface ElementNode {
  name: string;
  path: string;
  type: string;
  definition: string;
  referenceTypes?: string[];
  contains?: ElementNode[];
}

interface ReverseReferences {
  [resourceName: string]: ElementNode[];
}

export const resourceTree: ResourceTree = rawResourceTree;
export const complexTypesTree: ResourceTree = rawComplexTypesTree;
export const reverseReferences: ReverseReferences = rawReverseReferences;

export const getResource = (resourceName: string): ResourceNode => {
  if (!(resourceName in resourceTree)) {
    throw new Error(`Resource not found in resource tree (${resourceName})`);
  }
  return resourceTree[resourceName];
};

export const getComplexType = (typeName: string): ResourceNode => {
  if (!(typeName in complexTypesTree)) {
    throw new Error(`Type not found in complex type tree (${typeName})`);
  }
  return complexTypesTree[typeName];
};

export const getReverseReferences = (resourceName: string): ElementNode[] => {
  if (!(resourceName in reverseReferences)) {
    throw new Error(
      `Resource not found in reverse references (${resourceName})`
    );
  }
  return reverseReferences[resourceName];
};

export const getResolvedPath = (parentPath: string, path: string): string => {
  const pathComponents = path.split(".");
  return `${parentPath}.${pathComponents[pathComponents.length - 1]}`;
};

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
