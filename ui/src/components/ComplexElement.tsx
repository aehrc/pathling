/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { useState } from "react";

import {
  complexTypesTree,
  ElementNode,
  getResolvedPath
} from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import TreeNodeTooltip from "./TreeNodeTooltip";
import "./style/ComplexElement.scss";

interface Props extends ElementNode {
  parentPath: string;
}

function ComplexElement(props: Props) {
  const { name, type, path, definition, parentPath } = props,
    contains = complexTypesTree[type].contains,
    resolvedPath = getResolvedPath(parentPath, path),
    [isExpanded, setExpanded] = useState(false);

  return (
    <li className="complex-element">
      <div className="content">
        <span
          className={isExpanded ? "caret-open" : "caret-closed"}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className="icon" />
        <TreeNodeTooltip
          path={resolvedPath}
          type={type}
          definition={definition}
        >
          <span className="label">{name}</span>
        </TreeNodeTooltip>
      </div>
      {isExpanded ? (
        <ol className="contains">
          <ContainedElements nodes={contains} parentPath={resolvedPath} />
        </ol>
      ) : null}
    </li>
  );
}

export default ComplexElement;
