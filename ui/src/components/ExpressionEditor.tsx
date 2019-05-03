/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from "react";
import {
  Label,
  Popover,
  PopoverInteractionKind,
  Position,
  TextArea
} from "@blueprintjs/core";

import "./style/ExpressionEditor.scss";
import {
  Aggregation,
  Grouping,
  PartialAggregation,
  PartialGrouping
} from "../store/QueryReducer";

interface Props {
  expression: Aggregation | Grouping;
  onChange: (expression: PartialAggregation | PartialGrouping) => void;
  children: any;
}

function ExpressionEditor(props: Props) {
  const { expression, onChange, children } = props;

  const renderContent = () => {
    return (
      <div className="expression-form">
        <Label className="label-label">
          Label
          <input
            className="label-input"
            value={expression.label}
            onChange={event => onChange({ label: event.target.value })}
            onFocus={event => event.target.select()}
            autoFocus
          />
        </Label>
        <Label className="expression-label">
          Expression
          <TextArea
            className="expression-input"
            value={expression.expression}
            onChange={event => onChange({ expression: event.target.value })}
            onFocus={event => event.target.select()}
            growVertically
          />
        </Label>
      </div>
    );
  };

  return (
    <Popover
      content={renderContent()}
      position={Position.BOTTOM}
      boundary="viewport"
      interactionKind={PopoverInteractionKind.CLICK}
      className="expression-editor"
      targetClassName="target"
      popoverClassName="expression-editor-popover"
    >
      <>{children}</>
    </Popover>
  );
}

export default ExpressionEditor;
