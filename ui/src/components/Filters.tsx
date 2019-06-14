/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { ReactElement } from "react";
import { Icon, Tag } from "@blueprintjs/core";
import { connect } from "react-redux";

import * as queryActions from "../store/QueryActions";
import * as elementTreeActions from "../store/ElementTreeActions";
import ExpressionEditor from "./ExpressionEditor";
import { GlobalState } from "../store";
import {
  Aggregation,
  Filter,
  Grouping,
  PartialFilter
} from "../store/QueryReducer";
import "./style/Filters.scss";

interface Props {
  aggregations?: Aggregation[];
  groupings?: Grouping[];
  filters?: Filter[];
  removeFilter: (index: number) => any;
  updateFilter: (index: number, filter: PartialFilter) => any;
  clearElementTreeFocus: () => any;
}

/**
 * Renders a control which can be used to represent a set of selected
 * filters, when composing a query.
 *
 * @author John Grimes
 */
function Filters(props: Props) {
  const {
    aggregations,
    groupings,
    filters,
    removeFilter,
    updateFilter,
    clearElementTreeFocus
  } = props;

  const handleRemove = (event: any, index: number): void => {
    // This is required to stop the click event from opening the expression
    // editor for other filters.
    event.stopPropagation();
    if (aggregations.length + groupings.length + filters.length === 1) {
      clearElementTreeFocus();
    }
    removeFilter(index);
  };

  const handleChange = (i: number, filter: PartialFilter) => {
    updateFilter(i, filter);
  };

  const renderBlankCanvas = (): ReactElement => (
    <div className="filters__blank">Filters</div>
  );

  const renderFilters = (): ReactElement[] =>
    filters.map((filter, i) => (
      <ExpressionEditor
        key={i}
        expression={filter}
        onChange={filter => handleChange(i, filter)}
      >
        <Tag
          className="filters__expression"
          round={true}
          large={true}
          onRemove={event => handleRemove(event, i)}
        >
          {filter.label}
        </Tag>
      </ExpressionEditor>
    ));

  return (
    <div className="filters">
      <Icon className="filters__identity" icon="filter" />
      {filters.length === 0 ? renderBlankCanvas() : renderFilters()}
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
)(Filters);
