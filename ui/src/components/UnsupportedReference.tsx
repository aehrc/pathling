/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import { ElementNode, reverseReferences } from "../fhir/ResourceTree";
import "./style/UnsupportedReference.scss";
import TreeNodeTooltip from "./TreeNodeTooltip";

interface Props extends ElementNode {
  reverse?: boolean;
}

function UnsupportedReference(props: Props) {
  const { name, type, definition, referenceTypes, path, reverse } = props;

  const tooltipProps: any = {
    type,
    definition,
    note: "This server does not support this resource type."
  };
  if (type === "Reference") {
    tooltipProps.referenceTypes = referenceTypes;
  }

  return (
    <li
      className={
        reverse ? "unsupported-reference reverse" : "unsupported-reference"
      }
    >
      <TreeNodeTooltip {...tooltipProps}>
        <span className="caret-none" />
        <span className="icon" />
        <span className="label">{reverse ? path : name}</span>
      </TreeNodeTooltip>
    </li>
  );
}

export default UnsupportedReference;
