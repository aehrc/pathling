/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Icon, Tag } from "@blueprintjs/core";

import * as queryActions from "../store/QueryActions";
import * as elementTreeActions from "../store/ElementTreeActions";
import { Aggregation, PartialAggregation } from "../store/QueryReducer";
import { GlobalState } from "../store";
import "./style/Aggregations.scss";
import { ReactElement } from "react";
import ExpressionEditor from "./ExpressionEditor";

interface Props {
  aggregations?: Aggregation[];
  removeAggregation: (index: number) => any;
  updateAggregation: (index: number, aggregation: PartialAggregation) => any;
  setElementTreeFocus: (focus: string) => any;
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
    removeAggregation,
    updateAggregation,
    setElementTreeFocus,
    clearElementTreeFocus
  } = props;

  const getSubjectResourceFromExpression = (expression: string): string => {
    const subjectSearchResult = /^([A-Z][A-Za-z]+)/.exec(expression);
    return subjectSearchResult !== null ? subjectSearchResult[1] : null;
  };

  const handleRemove = (event: any, index: number): void => {
    // This is required to stop the click event from opening the expression
    // editor for other aggregations.
    event.stopPropagation();
    if (aggregations.length === 1) {
      clearElementTreeFocus();
    }
    removeAggregation(index);
  };

  const handleChange = (i: number, aggregation: PartialAggregation) => {
    updateAggregation(i, aggregation);
    const subjectResource = getSubjectResourceFromExpression(
      aggregation.expression
    );
    if (subjectResource !== null) {
      setElementTreeFocus(subjectResource);
    }
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
        <Tag
          round={true}
          large={true}
          onRemove={event => handleRemove(event, i)}
        >
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

const actions = { ...queryActions, ...elementTreeActions };

export default connect(
  mapStateToProps,
  actions
)(Aggregations);
