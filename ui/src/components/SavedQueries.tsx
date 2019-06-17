/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Button, ButtonGroup } from "@blueprintjs/core";
import React, { ReactElement } from "react";
import { connect } from "react-redux";

import { SavedQueriesWithStatuses } from "../store/SavedQueriesReducer";
import { GlobalState } from "../store";
import "./style/SavedQueries.scss";

interface Props {
  queries: SavedQueriesWithStatuses;
}

function SavedQueries(props: Props) {
  const { queries } = props;

  const renderQuery = (query: string, i: number): ReactElement => {
    return (
      <li key={i} className="saved-queries__query">
        <ButtonGroup className="saved-queries__query-actions" minimal>
          <Button icon="edit" title="Edit query name" small />
          <Button icon="trash" title="Delete query" small />
        </ButtonGroup>
        <div className="saved-queries__query-name">{query}</div>
      </li>
    );
  };

  return (
    <ol className="saved-queries">
      {Object.keys(queries).map((query, i) => renderQuery(query, i))}
    </ol>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  queries: state.savedQueries.queries
});

export default connect(mapStateToProps)(SavedQueries);
