/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { ReactElement } from "react";
import { connect } from "react-redux";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { Query } from "../store/QueryReducer";
import { deleteQuery, saveQuery } from "../store/SavedQueriesActions";

import {
  SavedQueriesWithStatuses,
  SavedQuery,
  SavedQueryWithStatus
} from "../store/SavedQueriesReducer";
import { GlobalState } from "../store";
import { loadQuery } from "../store/QueryActions";
import "./style/SavedQueries.scss";
import Alerter from "./Alerter";
import SavedQueryItem from "./SavedQueryItem";

interface Props {
  queries: SavedQueriesWithStatuses;
  loadedQueryId: string;
  loadQuery: (query: SavedQuery) => any;
  setElementTreeFocus: (focus: string) => any;
  deleteQuery: (id: string) => any;
  saveQuery: (query: Query) => any;
}

function SavedQueries(props: Props) {
  const {
    queries,
    loadedQueryId,
    loadQuery,
    setElementTreeFocus,
    deleteQuery
  } = props;

  const getFocusResource = (query: SavedQuery): string => {
    const {
        query: { aggregations, groupings, filters }
      } = query,
      allSubjectResources = aggregations
        .map(agg => getSubjectResourceFromExpression(agg.expression))
        .concat(
          groupings.map(group =>
            getSubjectResourceFromExpression(group.expression)
          )
        )
        .concat(
          filters.map(filter =>
            getSubjectResourceFromExpression(filter.expression)
          )
        );
    return allSubjectResources.length > 0 ? allSubjectResources[0] : null;
  };

  const handleQueryClick = (query: SavedQuery) => {
    const focusResource = getFocusResource(query);
    loadQuery(query);
    setElementTreeFocus(focusResource);
  };

  const handleDeleteClick = (query: SavedQuery) => {
    deleteQuery(query.id);
    Alerter.show({
      message: `Query \u201c${query.name}\u201d deleted`,
      intent: "success"
    });
    // TODO: Add back undo functionality.
    // Alerter.show({
    //   message: `Query \u201c${query.name}\u201d deleted`,
    //   intent: "success",
    //   action: {
    //     text: "Undo",
    //     onClick: () => saveQuery(query),
    //     icon: "undo"
    //   },
    //   timeout: 10000
    // });
  };

  const renderQuery = (query: SavedQueryWithStatus): ReactElement => {
    return (
      <SavedQueryItem
        query={query}
        loaded={query.id === loadedQueryId}
        onClick={handleQueryClick}
        onClickDelete={handleDeleteClick}
      />
    );
  };

  return queries.length > 0 ? (
    <ol className="saved-queries">{queries.map(renderQuery)}</ol>
  ) : (
    <div className="saved-queries__blank">No queries saved yet.</div>
  );
}

const mapStateToProps = (state: GlobalState) => ({
    queries: state.savedQueries.queries,
    loadedQueryId: state.query.id
  }),
  actions = { loadQuery, setElementTreeFocus, deleteQuery, saveQuery };

export default connect(
  mapStateToProps,
  actions
)(SavedQueries);
