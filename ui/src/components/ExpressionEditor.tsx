/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import {
  Label,
  Popover,
  PopoverInteractionKind,
  Position,
  Switch
} from "@blueprintjs/core";
import React, { FormEvent, useEffect, useRef } from "react";
import MonacoEditor from "react-monaco-editor";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import { receiveExpressionFocus } from "../store/QueryActions";
import { ExpressionWithIdentity } from "../store/QueryReducer";
import "./style/ExpressionEditor.scss";

interface Props {
  expression: ExpressionWithIdentity;
  onChange: (expression: ExpressionWithIdentity) => void;
  children: any;
  focusedExpression: string;
  receiveExpressionFocus: () => any;
}

function ExpressionEditor(props: Props) {
  const {
      expression,
      onChange,
      children,
      focusedExpression,
      receiveExpressionFocus
    } = props,
    popoverRef = useRef(null);

  const editorWillMount = (monaco: any) => {
    monaco.languages.register({
      id: "fhirPath"
    });

    monaco.languages.setLanguageConfiguration("fhirPath", {
      brackets: [["(", ")", "delimiter.parenthesis"]]
    });

    monaco.languages.setMonarchTokensProvider("fhirPath", {
      keywords: ["and", "or", "true", "false", "$this", "in", "contains"],
      operators: ["<", ">", "<=", ">=", "=", "!="],
      resource: /[A-Z][a-z]+/,
      element: /[a-z][a-zA-Z0-9]+/,
      functionInvocation: /@element\((.*)\)/,
      tokenizer: {
        expression: [
          { include: "@whitespace" },
          [/[()]/, "@brackets"],
          [/[^|()\s]+\|[^|()\s]+(\|[^()|\s]+)?/, "type"], // Coding literal
          [/'.*'/, "string"],
          [/@[0-9\-]+/, "variable.value"],
          [/and|or|true|false|\$this|in|contains/, "keyword"],
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
            onChange={event =>
              onChange({ id: expression.id, label: event.target.value })
            }
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
              onChange={value =>
                onChange({ id: expression.id, expression: value })
              }
              editorWillMount={editorWillMount}
              options={{
                minimap: { enabled: false },
                codeLens: false,
                wordWrap: "on",
                autoClosingBrackets: "always",
                fontSize: 13,
                links: false
              }}
            />
          </div>
        </Label>
        <Switch
          className="expression-editor__disabled-input"
          checked={!(expression.disabled === true)}
          labelElement={"Enabled?"}
          onChange={(event: FormEvent<HTMLInputElement>) =>
            onChange({
              id: expression.id,
              disabled: event.currentTarget.checked !== true
            })
          }
        />
      </div>
    );
  };

  useEffect(() => {
    if (expression.id === focusedExpression) {
      popoverRef.current.targetElement.click();
      receiveExpressionFocus();
    }
  });

  return (
    <Popover
      content={renderContent()}
      position={Position.BOTTOM}
      boundary="viewport"
      interactionKind={PopoverInteractionKind.CLICK}
      className="expression-editor"
      targetClassName="expression-editor__target"
      popoverClassName="expression-editor__popover"
      ref={popoverRef}
      autoFocus
    >
      <>{children}</>
    </Popover>
  );
}

const mapStateToProps = (state: GlobalState) => ({
    focusedExpression: state.query.focusedExpression
  }),
  actions = { receiveExpressionFocus };

export default connect(
  mapStateToProps,
  actions
)(ExpressionEditor);
