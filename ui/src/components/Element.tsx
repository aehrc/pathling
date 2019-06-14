/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { ContextMenu, Menu } from "@blueprintjs/core";

import { ElementNode, getResolvedPath } from "../fhir/ResourceTree";
import AddAggregation from "./AddAggregation";
import AddGrouping from "./AddGrouping";
import "./style/Element.scss";
import TreeNodeTooltip from "./TreeNodeTooltip";
import AddFilter from "./AddFilter";

interface Props extends ElementNode {
  parentPath: string;
}

function Element(props: Props) {
  const { name, type, definition, path, parentPath } = props,
    resolvedPath = getResolvedPath(parentPath, path);

  const openContextMenu = (event: any): void => {
    ContextMenu.show(
      <Menu>
        <AddAggregation path={resolvedPath} />
        <AddGrouping path={resolvedPath} />
        <AddFilter path={resolvedPath} />
      </Menu>,
      {
        left: event.target.getBoundingClientRect().right,
        top: event.target.getBoundingClientRect().top
      }
    );
  };

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  return (
    <li className="element">
      <div className="content">
        <span className="caret-none" />
        <span className="icon" />
        <TreeNodeTooltip
          path={resolvedPath}
          type={type}
          definition={definition}
        >
          <span className="label">{name}</span>
        </TreeNodeTooltip>
        <span
          className="action"
          title={`Show actions relating to ${resolvedPath}`}
          onClick={openContextMenu}
          onKeyDown={handleTabIndexedKeyDown}
          tabIndex={0}
        />
      </div>
    </li>
  );
}

export default Element;
