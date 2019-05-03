/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as actions from "../store/QueryActions";
import { Aggregation, PartialAggregation } from "../store/QueryReducer";
import { GlobalState } from "../store";
import "./style/Aggregations.scss";
import { ReactElement } from "react";
import ExpressionEditor from "./ExpressionEditor";

interface Props {
  aggregations?: Aggregation[];
  removeAggregation: (index: number) => void;
  updateAggregation: (index: number, aggregation: PartialAggregation) => void;
}

/**
 * Renders a control which can be used to represent a set of selected
 * aggregations, when composing a query.
 *
 * @author John Grimes
 */
function Aggregations(props: Props) {
  const { aggregations, removeAggregation, updateAggregation } = props;

  const handleRemove = (index: number): void => {
    removeAggregation(index);
  };

  const handleChange = (i: number, aggregation: PartialAggregation) => {
    updateAggregation(i, aggregation);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="blank-canvas">Aggregations</div>
  );

  const renderAggregations = (): ReactElement[] =>
    aggregations.map((aggregation, i) => (
      <ExpressionEditor
        key={i}
        expression={aggregation}
        onChange={aggregation => handleChange(i, aggregation)}
      >
        <Tag round={true} large={true} onRemove={() => handleRemove(i)}>
          {aggregation.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="aggregations">
      <Icon className="section-identity" icon="trending-up" />
      {aggregations.length === 0 ? renderBlankCanvas() : renderAggregations()}
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  aggregations: state.query.aggregations
});

export default connect(
  mapStateToProps,
  actions
)(Aggregations);
