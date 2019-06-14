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
    <div className="tree-node-tooltip__content">
      <div className="tree-node-tooltip__path">{path}</div>
      <div className="tree-node-tooltip__definition">{definition}</div>
      <div className="tree-node-tooltip__type">{typeDescription}</div>
      {note ? <div className="tree-node-tooltip__note">{note}</div> : null}
    </div>
  );

  return (
    <Popover
      content={renderContent()}
      position={Position.RIGHT}
      boundary="viewport"
      interactionKind={PopoverInteractionKind.CLICK}
      className="tree-node-tooltip"
      targetClassName="tree-node-tooltip__target"
      popoverClassName="tree-node-tooltip__popover"
      hoverOpenDelay={300}
      lazy
    >
      <>{children}</>
    </Popover>
  );
}

export default TreeNodeTooltip;
