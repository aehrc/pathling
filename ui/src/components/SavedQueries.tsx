/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Button, ButtonGroup } from "@blueprintjs/core";
import React, { ReactElement } from "react";
import { connect } from "react-redux";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { deleteQuery, saveQuery } from "../store/SavedQueriesActions";

import {
  SavedQueriesWithStatuses,
  SavedQuery
} from "../store/SavedQueriesReducer";
import { GlobalState } from "../store";
import { loadQuery } from "../store/QueryActions";
import "./style/SavedQueries.scss";
import Alerter from "./Alerter";

interface Props {
  queries: SavedQueriesWithStatuses;
  loadedQueryName: string;
  loadQuery: (name: string, query: SavedQuery) => any;
  setElementTreeFocus: (focus: string) => any;
  deleteQuery: (name: string) => any;
  saveQuery: (name: string, query: SavedQuery) => any;
}

function SavedQueries(props: Props) {
  const {
    queries,
    loadedQueryName,
    loadQuery,
    setElementTreeFocus,
    deleteQuery,
    saveQuery
  } = props;

  const getFocusResource = (query: SavedQuery): string => {
    const allSubjectResources = query.aggregations
      .map(agg => getSubjectResourceFromExpression(agg.expression))
      .concat(
        query.groupings.map(group =>
          getSubjectResourceFromExpression(group.expression)
        )
      )
      .concat(
        query.filters.map(filter =>
          getSubjectResourceFromExpression(filter.expression)
        )
      );
    return allSubjectResources.length > 0 ? allSubjectResources[0] : null;
  };

  const handleQueryClick = (query: string) => {
    const focusResource = getFocusResource(queries[query].query);
    loadQuery(query, queries[query].query);
    setElementTreeFocus(focusResource);
  };

  const handleDeleteClick = (event: any, query: string) => {
    event.stopPropagation();
    deleteQuery(query);
    Alerter.show({
      message: `Query \u201c${query}\u201d deleted`,
      intent: "success",
      action: {
        text: "Undo",
        onClick: () => saveQuery(query, queries[query].query),
        icon: "undo"
      },
      timeout: 10000
    });
  };

  const renderQuery = (query: string, i: number): ReactElement => {
    return (
      <li
        key={i}
        className={
          query === loadedQueryName
            ? "saved-queries__query saved-queries__query--loaded"
            : "saved-queries__query"
        }
        onClick={
          query === loadedQueryName ? null : () => handleQueryClick(query)
        }
        title="Load this query"
      >
        <ButtonGroup className="saved-queries__query-actions" minimal>
          <Button icon="edit" title="Edit query name" small />
          <Button
            icon="trash"
            title="Delete query"
            small
            onClick={(event: any) => handleDeleteClick(event, query)}
          />
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
    queries: state.savedQueries.queries,
    loadedQueryName: state.query.name
  }),
  actions = { loadQuery, setElementTreeFocus, deleteQuery, saveQuery };

export default connect(
  mapStateToProps,
  actions
)(SavedQueries);
