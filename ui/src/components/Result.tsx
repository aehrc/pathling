/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Spinner, HTMLTable } from "@blueprintjs/core";
import isEqual from "lodash.isequal";

import "./style/Result.scss";
import { Parameter } from "../fhir/Types";
import { Query } from "../store/QueryReducer";
import { ReactElement } from "react";
import { GlobalState } from "../store";

interface Props {
  loading: boolean;
  groupings: Parameter[];
  query: Query;
  stale: boolean;
}

/**
 * Renders the result of a query as a table.
 *
 * @author John Grimes
 */
function Result(props: Props) {
  const { loading, groupings, query, stale } = props;

  const renderLoading = () => (
    <Spinner className="result__loading" size={100} intent="primary" />
  );

  const renderPart = (part: Parameter, i: number): ReactElement => {
    const key = Object.keys(part).find(key => key.match(/^value/) !== null),
      value = part[key];
    if (value === undefined) {
      return <td key={i}>(no value)</td>;
    } else if (key === "valueDate") {
      const date = new Date(value).toLocaleDateString();
      return <td key={i}>{date}</td>;
    } else if (key === "valueDateTime") {
      const date = new Date(value).toLocaleString();
      return <td key={i}>{date}</td>;
    } else {
      return <td key={i}>{part[key].toString()}</td>;
    }
  };

  const renderGrouping = (grouping: Parameter, i: number): ReactElement => {
    const parts = grouping.part.map((part, i) => renderPart(part, i));
    return <tr key={i}>{parts}</tr>;
  };

  const renderGroupings = (): ReactElement => {
    const groupHeadings = query.groupings.map(grouping => (
        <th key={grouping.label}>{grouping.label}</th>
      )),
      aggregationHeadings = query.aggregations.map(aggregation => (
        <th key={aggregation.label}>{aggregation.label}</th>
      )),
      rows = groupings.map((grouping, i) => renderGrouping(grouping, i));
    return rows.length > 0 ? (
      <HTMLTable interactive={true}>
        <thead>
          <tr>{groupHeadings.concat(aggregationHeadings)}</tr>
        </thead>
        <tbody>{rows}</tbody>
      </HTMLTable>
    ) : (
      <p>The result of this query contains zero groupings.</p>
    );
  };

  let content = null;
  if (loading) {
    content = renderLoading();
  } else if (groupings !== null) {
    content = renderGroupings();
  }
  return (
    <div className={stale ? "result result--stale" : "result"}>{content}</div>
  );
}

function checkStale(state: GlobalState): boolean {
  return !(
    state.result.query !== null && isEqual(state.query, state.result.query)
  );
}

const mapStateToProps = (state: GlobalState) => ({
  ...state.result,
  stale: checkStale(state)
});

export default connect(mapStateToProps)(Result);
