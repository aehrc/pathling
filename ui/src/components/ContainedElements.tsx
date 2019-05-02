/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from "react";

import { ElementNode, supportedComplexTypes } from "../fhir/ResourceTree";
import Reference from "./Reference";
import ComplexElement from "./ComplexElement";
import Element from "./Element";
import BackboneElement from "./BackboneElement";

interface Props {
  nodes: ElementNode[];
  parentPath: string;
}

function ContainedElements(props: Props) {
  const { nodes, parentPath } = props;
  if (!nodes) return null;

  const childNodes = nodes.map((node, i) => {
    const { type, contains } = node;
    if (type === "Reference") {
      return <Reference {...node} key={i} />;
    } else if (supportedComplexTypes.includes(type)) {
      return <ComplexElement {...node} key={i} parentPath={parentPath} />;
    } else if (contains) {
      return <BackboneElement {...node} key={i} parentPath={parentPath} />;
    } else {
      return <Element {...node} key={i} parentPath={parentPath} />;
    }
  });
  return <React.Fragment>{childNodes}</React.Fragment>;
}

export default ContainedElements;
