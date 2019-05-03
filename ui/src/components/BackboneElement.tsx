/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { useState } from "react";

import { ElementNode, getResolvedPath } from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import TreeNodeTooltip from "./TreeNodeTooltip";
import "./style/BackboneElement.scss";

interface Props extends ElementNode {
  parentPath: string;
}

function BackboneElement(props: Props) {
  const { name, type, path, definition, contains, parentPath } = props,
    resolvedPath = getResolvedPath(parentPath, path),
    [isExpanded, setExpanded] = useState(false);

  return (
    <li className="backbone-element">
      <TreeNodeTooltip path={resolvedPath} type={type} definition={definition}>
        <span
          className={isExpanded ? "caret-open" : "caret-closed"}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className="icon" />
        <span className="label">{name}</span>
      </TreeNodeTooltip>
      {isExpanded ? (
        <ol className="contains">
          <ContainedElements nodes={contains} parentPath={resolvedPath} />
        </ol>
      ) : null}
    </li>
  );
}

export default BackboneElement;
