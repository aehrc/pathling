/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from "react";
import {
  Label,
  Popover,
  PopoverInteractionKind,
  Position
} from "@blueprintjs/core";

import "./style/ExpressionEditor.scss";
import {
  Aggregation,
  Filter,
  Grouping,
  PartialAggregation,
  PartialFilter,
  PartialGrouping
} from "../store/QueryReducer";
import MonacoEditor from "react-monaco-editor";

interface Props {
  expression: Aggregation | Grouping | Filter;
  onChange: (
    expression: PartialAggregation | PartialGrouping | PartialFilter
  ) => void;
  children: any;
}

function ExpressionEditor(props: Props) {
  const { expression, onChange, children } = props;

  const editorWillMount = (monaco: any) => {
    monaco.languages.register({
      id: "fhirPath"
    });

    monaco.languages.setLanguageConfiguration("fhirPath", {
      brackets: [["(", ")", "delimiter.parenthesis"]]
    });

    monaco.languages.setMonarchTokensProvider("fhirPath", {
      keywords: ["and", "or", "true", "false"],
      operators: ["<", ">", "<=", ">=", "=", "!="],
      resource: /[A-Z][a-z]+/,
      element: /[a-z][a-zA-Z0-9]+/,
      functionInvocation: /@element\((.*)\)/,
      tokenizer: {
        expression: [
          { include: "@whitespace" },
          [/[()]/, "@brackets"],
          [/'.*'/, "string"],
          [/@[0-9\-]+/, "variable.value"],
          [/and|or|true|false/, "keyword"],
          [/<|>|<=|>=|=|!=/, "operators"],
          [/@element/, "variable.name"],
          [/@resource/, "constant"]
        ],
        whitespace: [
          [/[ \t\r\n]+/, ""],
          [/\/\*/, "comment", "@comment"],
          [/\/\/.*$/, "comment"]
        ],
        comment: [
          [/[^\/*]+/, "comment"],
          [/\*\//, "comment", "@pop"],
          [/[\/*]/, "comment"]
        ]
      }
    });
  };

  const renderContent = () => {
    return (
      <div className="expression-editor__form">
        <Label className="expression-editor__label-label">
          Label
          <input
            className="expression-editor__label-input"
            value={expression.label}
            onChange={event => onChange({ label: event.target.value })}
            onFocus={event => event.target.select()}
            autoFocus
          />
        </Label>
        <Label className="expression-editor__expression-label">
          Expression
          <div className="expression-editor__expression-input">
            <MonacoEditor
              language="fhirPath"
              value={expression.expression}
              onChange={expression => onChange({ expression })}
              editorWillMount={editorWillMount}
              options={{
                minimap: { enabled: false },
                codeLens: false,
                wordWrap: "on",
                autoClosingBrackets: "always",
                fontSize: 13
              }}
            />
          </div>
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
      targetClassName="expression-editor__target"
      popoverClassName="expression-editor__popover"
      autoFocus
    >
      <>{children}</>
    </Popover>
  );
}

export default ExpressionEditor;
