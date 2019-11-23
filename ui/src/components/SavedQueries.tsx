/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { ReactElement } from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import { loadQuery, setSubjectResource } from "../store/QueryActions";
import { Query } from "../store/QueryReducer";
import {
  cancelEditingSavedQuery,
  deleteQuery,
  editSavedQuery,
  saveQuery,
  updateQuery
} from "../store/SavedQueriesActions";
import {
  SavedQueriesWithStatuses,
  SavedQuery,
  SavedQueryWithStatus
} from "../store/SavedQueriesReducer";
import Alerter from "./Alerter";
import EditableQueryItem from "./EditableQueryItem";
import SavedQueryItem from "./SavedQueryItem";
import "./style/SavedQueries.scss";

interface Props {
  queries: SavedQueriesWithStatuses;
  loadedQueryId: string;
  loadQuery: (query: SavedQuery) => any;
  setSubjectResource: (subjectResource: string) => any;
  deleteQuery: (id: string) => any;
  saveQuery: (query: Query) => any;
  updateQuery: (query: SavedQuery) => any;
  editSavedQuery: (id: string) => any;
  cancelEditingSavedQuery: (id: string) => any;
}

function SavedQueries(props: Props) {
  const {
    queries,
    loadedQueryId,
    loadQuery,
    deleteQuery,
    updateQuery,
    editSavedQuery,
    cancelEditingSavedQuery
  } = props;

  const handleClickQuery = (query: SavedQuery) => {
    loadQuery(query);
  };

  const handleClickEdit = (query: SavedQuery) => {
    editSavedQuery(query.id);
  };

  const handleClickDelete = (query: SavedQuery) => {
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

  const handleClickAccept = (query: SavedQuery) => {
    updateQuery(query);
  };

  const handleClickCancel = (query: SavedQuery) => {
    cancelEditingSavedQuery(query.id);
  };

  const renderQuery = (query: SavedQueryWithStatus): ReactElement => {
    return query.status === "editing" ? (
      <EditableQueryItem
        key={query.id}
        query={query}
        onClickAccept={handleClickAccept}
        onClickCancel={handleClickCancel}
      />
    ) : (
      <SavedQueryItem
        key={query.id}
        query={query}
        loaded={query.id === loadedQueryId}
        onClick={handleClickQuery}
        onClickEdit={handleClickEdit}
        onClickDelete={handleClickDelete}
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
  actions = {
    loadQuery,
    setSubjectResource,
    deleteQuery,
    saveQuery,
    updateQuery,
    editSavedQuery,
    cancelEditingSavedQuery
  };

export default connect(
  mapStateToProps,
  actions
)(SavedQueries);
