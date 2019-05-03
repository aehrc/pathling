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
import * as actions from "../store/QueryActions";
import "./style/Resource.scss";

interface Props extends ResourceNode {
  name: string;
  parentPath?: string;
  addAggregation: (expression: string) => void;
}

function Resource(props: Props) {
  const { name, definition, contains, parentPath, addAggregation } = props,
    aggregationExpression = `${name}.count()`,
    [isExpanded, setExpanded] = useState(false);

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
    <li className="resource">
      <div className="content">
        <span
          className={isExpanded ? "caret-open" : "caret-closed"}
          onClick={() => setExpanded(!isExpanded)}
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
            onClick={() => addAggregation(aggregationExpression)}
          />
        )}
      </div>
      {isExpanded ? <ol className="contains">{renderContains()}</ol> : null}
    </li>
  );
}

export default connect(
  null,
  actions
)(Resource);
