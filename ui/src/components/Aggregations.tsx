/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as queryActions from "../store/QueryActions";
import * as elementTreeActions from "../store/ElementTreeActions";
import {
  Aggregation,
  Filter,
  Grouping,
  PartialAggregation
} from "../store/QueryReducer";
import { GlobalState } from "../store";
import "./style/Aggregations.scss";
import { ReactElement } from "react";
import ExpressionEditor from "./ExpressionEditor";

interface Props {
  aggregations?: Aggregation[];
  groupings?: Grouping[];
  filters?: Filter[];
  removeAggregation: (index: number) => any;
  updateAggregation: (index: number, aggregation: PartialAggregation) => any;
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

  const handleRemove = (event: any, index: number): void => {
    // This is required to stop the click event from opening the expression
    // editor for other aggregations.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeAggregation(index);
  };

  const handleChange = (i: number, aggregation: PartialAggregation) => {
    updateAggregation(i, aggregation);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="aggregations__blank">Aggregations</div>
  );

  const renderAggregations = (): ReactElement[] =>
    aggregations.map((aggregation, i) => (
      <ExpressionEditor
        key={i}
        expression={aggregation}
        onChange={aggregation => handleChange(i, aggregation)}
      >
        <Tag
          className="aggregations__expression"
          round={true}
          large={true}
          onRemove={event => handleRemove(event, i)}
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
