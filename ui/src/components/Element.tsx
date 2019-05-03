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
      </Menu>,
      {
        left: event.clientX,
        top: event.clientY
      }
    );
  };

  return (
    <li className="element">
      <TreeNodeTooltip path={resolvedPath} type={type} definition={definition}>
        <span className="caret-none" />
        <span className="icon" />
        <span className="label">{name}</span>
        <span className="action" onClick={openContextMenu} />
      </TreeNodeTooltip>
    </li>
  );
}

export default Element;
