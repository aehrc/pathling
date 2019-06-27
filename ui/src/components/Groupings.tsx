/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Icon, Tag } from "@blueprintjs/core";
import * as React from "react";
import { MouseEvent, ReactElement } from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import * as elementTreeActions from "../store/ElementTreeActions";
import * as queryActions from "../store/QueryActions";
import { ExpressionWithIdentity } from "../store/QueryReducer";
import ExpressionEditor from "./ExpressionEditor";
import "./style/Groupings.scss";

interface Props {
  aggregations: ExpressionWithIdentity[];
  groupings: ExpressionWithIdentity[];
  filters: ExpressionWithIdentity[];
  removeGrouping: (id: string) => any;
  updateGrouping: (grouping: ExpressionWithIdentity) => any;
  clearElementTreeFocus: () => any;
}

/**
 * Renders a list of currently selected groupings, used when composing a query.
 *
 * @author John Grimes
 */
function Groupings(props: Props) {
  const {
    aggregations,
    groupings,
    filters,
    removeGrouping,
    updateGrouping,
    clearElementTreeFocus
  } = props;

  const handleRemove = (
    event: MouseEvent,
    grouping: ExpressionWithIdentity
  ) => {
    // This is required to stop the click event from opening the expression
    // editor for other groupings.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeGrouping(grouping.id);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="groupings__blank">Groupings</div>
  );

  const renderGroupings = (): ReactElement[] =>
    groupings.map((grouping, i) => (
      <ExpressionEditor key={i} expression={grouping} onChange={updateGrouping}>
        <Tag
          className="groupings__expression"
          round={true}
          large={true}
          onRemove={event => handleRemove(event, grouping)}
          title="Edit this expression"
        >
          {grouping.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="groupings">
      <Icon className="groupings__identity" icon="graph" />
      {groupings.length === 0 ? renderBlankCanvas() : renderGroupings()}
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({ ...state.query.query });

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Groupings);
