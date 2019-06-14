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

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  return (
    <li className="backbone-element">
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

export default BackboneElement;
