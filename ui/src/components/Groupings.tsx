/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as queryActions from "../store/QueryActions";
import * as elementTreeActions from "../store/ElementTreeActions";
import { Aggregation, Grouping, PartialGrouping } from "../store/QueryReducer";
import { GlobalState } from "../store";
import { ReactElement } from "react";
import "./style/Groupings.scss";
import ExpressionEditor from "./ExpressionEditor";

interface Props {
  aggregations: Aggregation[];
  groupings: Grouping[];
  filters: Grouping[];
  removeGrouping: (index: number) => void;
  updateGrouping: (index: number, grouping: PartialGrouping) => void;
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

  const handleRemove = (event: any, index: number): void => {
    // This is required to stop the click event from opening the expression
    // editor for other groupings.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeGrouping(index);
  };

  const handleChange = (i: number, grouping: PartialGrouping) => {
    updateGrouping(i, grouping);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="groupings__blank">Groupings</div>
  );

  const renderGroupings = (): ReactElement[] =>
    groupings.map((grouping, i) => (
      <ExpressionEditor
        key={i}
        expression={grouping}
        onChange={grouping => handleChange(i, grouping)}
      >
        <Tag
          className="groupings__expression"
          round={true}
          large={true}
          onRemove={event => handleRemove(event, i)}
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

const mapStateToProps = (state: GlobalState) => ({
  aggregations: state.query.aggregations,
  groupings: state.query.groupings,
  filters: state.query.filters
});

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Groupings);
