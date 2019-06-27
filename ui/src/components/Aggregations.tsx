/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Icon, Tag } from "@blueprintjs/core";
import * as React from "react";
import { ReactElement } from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import * as elementTreeActions from "../store/ElementTreeActions";
import * as queryActions from "../store/QueryActions";
import { ExpressionWithIdentity } from "../store/QueryReducer";
import ExpressionEditor from "./ExpressionEditor";
import "./style/Aggregations.scss";

interface Props {
  aggregations: ExpressionWithIdentity[];
  groupings: ExpressionWithIdentity[];
  filters: ExpressionWithIdentity[];
  removeAggregation: (id: string) => any;
  updateAggregation: (aggregation: ExpressionWithIdentity) => any;
  clearElementTreeFocus: () => any;
}

/**
 * Renders a control which can be used to represent a set of selected
 * aggregations, when composing a query.
 *
 * @author John Grimes
 */
function Aggregations(props: Props) {
  const {
    aggregations,
    groupings,
    filters,
    removeAggregation,
    updateAggregation,
    clearElementTreeFocus
  } = props;

  const handleRemove = (event: any, aggregation: ExpressionWithIdentity) => {
    // This is required to stop the click event from opening the expression
    // editor for other aggregations.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeAggregation(aggregation.id);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="aggregations__blank">Aggregations</div>
  );

  const renderAggregations = (): ReactElement[] =>
    aggregations.map((aggregation, i) => (
      <ExpressionEditor
        key={i}
        expression={aggregation}
        onChange={updateAggregation}
      >
        <Tag
          className="aggregations__expression"
          round={true}
          large={true}
          onRemove={event => handleRemove(event, aggregation)}
          title="Edit this expression"
        >
          {aggregation.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="aggregations">
      <Icon className="aggregations__identity" icon="trending-up" />
      {aggregations.length === 0 ? renderBlankCanvas() : renderAggregations()}
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({ ...state.query.query });

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Aggregations);
