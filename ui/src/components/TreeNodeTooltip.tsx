/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from "react";
import { Popover, PopoverInteractionKind, Position } from "@blueprintjs/core";

import "./style/TreeNodeTooltip.scss";

export type TreeNodeTooltipProps = RegularProps | ReferenceProps;

interface RegularProps {
  path: string;
  type: string;
  definition: string;
  referenceTypes?: string[];
  note?: string;
  children?: any;
}

interface ReferenceProps extends RegularProps {
  type: "Reference";
  referenceTypes: string[];
}

function TreeNodeTooltip(props: TreeNodeTooltipProps) {
  const { path, type, definition, referenceTypes, note, children } = props,
    typeDescription =
      type === "Reference" ? `Reference (${referenceTypes.join(" | ")})` : type;

  const renderContent = () => (
    <div className="content">
      <div className="path">{path}</div>
      <div className="definition">{definition}</div>
      <div className="type">{typeDescription}</div>
      {note ? <div className="note">{note}</div> : null}
    </div>
  );

  return (
    <Popover
      content={renderContent()}
      position={Position.RIGHT}
      boundary="viewport"
      interactionKind={PopoverInteractionKind.HOVER}
      className="tooltip"
      targetClassName="content"
      popoverClassName="tree-node-tooltip-popover"
      hoverOpenDelay={300}
      lazy
    >
      <>{children}</>
    </Popover>
  );
}

export default TreeNodeTooltip;
