/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from "react";
import { connect } from "react-redux";

import {
  getReverseReferences,
  ResourceNode,
  reverseReferences
} from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import ReverseReference from "./ReverseReference";
import TreeNodeTooltip from "./TreeNodeTooltip";
import * as queryActions from "../store/QueryActions";
import * as elementTreeActions from "../store/ElementTreeActions";
import "./style/Resource.scss";
import { GlobalState } from "../store";

interface Props extends ResourceNode {
  name: string;
  parentPath?: string;
  disabled?: boolean;
  focus?: string;
  addAggregation: (expression: string) => any;
  setElementTreeFocus: (focus: string) => any;
}

function Resource(props: Props) {
  const {
      name,
      definition,
      contains,
      parentPath,
      disabled,
      focus,
      addAggregation,
      setElementTreeFocus
    } = props,
    aggregationExpression = `${name}.count()`,
    [isExpanded, setExpanded] = useState(false);

  const getExpanded = () => isExpanded && !disabled;

  const handleClickAction = () => {
    if (disabled) return;
    addAggregation(aggregationExpression);
    if (focus === null) setElementTreeFocus(name);
  };

  const renderContains = () => {
    const newParentPath = parentPath ? parentPath : name,
      reverseReferenceNodes =
        name in reverseReferences
          ? getReverseReferences(name).map((node, i) => (
              <ReverseReference
                {...node}
                key={i + 1}
                parentPath={newParentPath}
              />
            ))
          : [];
    return [
      <ContainedElements key={0} nodes={contains} parentPath={newParentPath} />
    ].concat(reverseReferenceNodes);
  };

  return (
    <li className={disabled ? "resource disabled" : "resource"}>
      <div className="content">
        <span
          className={getExpanded() ? "caret-open" : "caret-closed"}
          onClick={disabled ? null : () => setExpanded(!isExpanded)}
        />
        <span className="icon" />
        <TreeNodeTooltip
          path={parentPath ? parentPath : name}
          type="Resource"
          definition={definition}
        >
          <span className="label">{name}</span>
        </TreeNodeTooltip>
        {parentPath ? null : (
          <span
            className="action"
            title={`Add ${aggregationExpression} to aggregations`}
            onClick={handleClickAction}
          />
        )}
      </div>
      {getExpanded() ? <ol className="contains">{renderContains()}</ol> : null}
    </li>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  focus: state.elementTree.focus
});

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Resource);
