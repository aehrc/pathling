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
    prefix = parentPath == null ? "" : `${parentPath}.`,
    pathComponents = path.split("."),
    sourceType = pathComponents[0],
    resolvedPath = `${prefix}reverseResolve(${path})`,
    unsupported = !(sourceType in resourceTree),
    [isExpanded, setExpanded] = useState(false);

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  const renderContains = () => {
    const contains = getResource(sourceType).contains,
      reverseReferenceNodes =
        sourceType in reverseReferences
          ? getReverseReferences(sourceType).map((node, i) => (
              <ReverseReference
                {...node}
                key={i + 1}
                parentPath={resolvedPath}
              />
            ))
          : [];
    return [
      <ContainedElements nodes={contains} key={0} parentPath={resolvedPath} />
    ].concat(reverseReferenceNodes);
  };

  return unsupported ? (
    <UnsupportedReference {...props} reverse />
  ) : (
    <li className="reverse-reference">
      <div className="content">
        <span
          className={isExpanded ? "caret-open" : "caret-closed"}
          title={`Show children of ${resolvedPath}`}
          onClick={() => setExpanded(!isExpanded)}
          onKeyDown={handleTabIndexedKeyDown}
          tabIndex={0}
        />
        <span className="icon" />
        <TreeNodeTooltip
          path={resolvedPath}
          type={type}
          definition={definition}
          referenceTypes={referenceTypes}
        >
          <span className="label">{path}</span>
        </TreeNodeTooltip>
      </div>
      {isExpanded ? <ol className="contains">{renderContains()}</ol> : null}
    </li>
  );
}

export default ReverseReference;
