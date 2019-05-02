/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { useState } from "react";

import {
  ElementNode,
  getResource,
  getReverseReferences,
  resourceTree,
  reverseReferences
} from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import UnsupportedReference from "./UnsupportedReference";
import TreeNodeTooltip from "./TreeNodeTooltip";
import "./style/ReverseReference.scss";

interface Props extends ElementNode {
  parentPath: string;
}

function ReverseReference(props: Props) {
  const { path, type, definition, referenceTypes, parentPath } = props,
    pathComponents = path.split("."),
    sourceType = pathComponents[0],
    unsupported = !(sourceType in resourceTree),
    [isExpanded, setExpanded] = useState(false);

  const renderContains = () => {
    const contains = getResource(sourceType).contains,
      newParentPath = `${parentPath}.reverseResolve(${path})`,
      reverseReferenceNodes =
        sourceType in reverseReferences
          ? getReverseReferences(sourceType).map((node, i) => (
              <ReverseReference
                {...node}
                key={i + 1}
                parentPath={newParentPath}
              />
            ))
          : [];
    return [
      <ContainedElements nodes={contains} key={0} parentPath={newParentPath} />
    ].concat(reverseReferenceNodes);
  };

  return unsupported ? (
    <UnsupportedReference {...props} reverse />
  ) : (
    <li className="reverse-reference">
      <TreeNodeTooltip
        type={type}
        definition={definition}
        referenceTypes={referenceTypes}
      >
        <span
          className={isExpanded ? "caret-open" : "caret-closed"}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className="icon" />
        <span className="label">{path}</span>
      </TreeNodeTooltip>
      {isExpanded ? <ol className="contains">{renderContains()}</ol> : null}
    </li>
  );
}

export default ReverseReference;
