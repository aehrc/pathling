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
import store, { GlobalState } from "../store";
import * as queryActions from "../store/QueryActions";
import { addAggregation } from "../store/QueryActions";
import ContainedElements from "./ContainedElements";
import ReverseReference from "./ReverseReference";
import "./style/Resource.scss";
import TreeNodeTooltip from "./TreeNodeTooltip";

interface Props extends ResourceNode {
  name: string;
  parentPath?: string;
  disabled?: boolean;
  subjectResource?: string;
  setSubjectResource: (subjectResource: string) => any;
  focusExpression: (id: string) => any;
}

function Resource(props: Props) {
  const {
      name,
      definition,
      contains,
      parentPath,
      disabled,
      subjectResource,
      setSubjectResource,
      focusExpression
    } = props,
    aggregationExpression = `${name}.count()`,
    [isExpanded, setExpanded] = useState(false);

  const getExpanded = () => isExpanded && !disabled;

  const handleClickAction = () => {
    if (disabled) return;
    const addAggregationAction = addAggregation({
      label: aggregationExpression,
      expression: aggregationExpression
    });
    store.dispatch(addAggregationAction);
    if (subjectResource === null) setSubjectResource(name);
    focusExpression(addAggregationAction.aggregation.id);
  };

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
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
          title={`Show children of ${name}`}
          onClick={disabled ? null : () => setExpanded(!isExpanded)}
          onKeyDown={handleTabIndexedKeyDown}
          tabIndex={disabled ? -1 : 0}
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
            onKeyDown={handleTabIndexedKeyDown}
            tabIndex={disabled ? -1 : 0}
          />
        )}
      </div>
      {getExpanded() ? <ol className="contains">{renderContains()}</ol> : null}
    </li>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  subjectResource: state.query.query.subjectResource
});

const actions = { ...queryActions };

export default connect(
  mapStateToProps,
  actions
)(Resource);
