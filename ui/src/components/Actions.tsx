/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Alignment, Button, Intent, Navbar } from "@blueprintjs/core";
import * as React from "react";
import { connect } from "react-redux";
import { GlobalState } from "../store";
import { clearElementTreeFocus } from "../store/ElementTreeActions";
import { clearQuery } from "../store/QueryActions";
import { Query, QueryState } from "../store/QueryReducer";
import {
  cancelAndClearResult,
  clearResult,
  fetchQueryResult
} from "../store/ResultActions";
import { ResultState } from "../store/ResultReducer";
import { saveQuery, updateQuery } from "../store/SavedQueriesActions";
import { SavedQuery } from "../store/SavedQueriesReducer";
import Alerter from "./Alerter";
import "./style/Actions.scss";

interface Props {
  query: QueryState;
  result: ResultState;
  fhirServer?: string;
  fetchQueryResult?: (fhirServer: string) => any;
  clearQuery?: () => any;
  clearResult?: () => any;
  cancelAndClearResult?: () => any;
  clearElementTreeFocus?: () => any;
  saveQuery?: (query: Query) => any;
  updateQuery?: (query: SavedQuery) => any;
}

/**
 * Renders a toolbar containing actions relating to the currently entered query.
 *
 * @author John Grimes
 */
function Actions(props: Props) {
  const {
    fetchQueryResult,
    clearQuery,
    clearResult,
    cancelAndClearResult,
    clearElementTreeFocus,
    saveQuery,
    query: queryState,
    query: { query },
    query: {
      query: { aggregations, groupings }
    },
    result: { loading, executionTime },
    fhirServer
  } = props;

  const queryIsEmpty = (): boolean =>
    aggregations.length === 0 && groupings.length === 0;

  const handleClickExecute = () => {
    if (!fhirServer) {
      Alerter.show({
        message: "Missing FHIR server configuration value",
        intent: "danger"
      });
    } else {
      fetchQueryResult(fhirServer);
    }
  };

  const handleClickClearQuery = () => {
    clearQuery();
    clearResult();
    clearElementTreeFocus();
  };

  const handleCancelQuery = () => {
    cancelAndClearResult();
  };

  const handleClickSave = () => {
    if (queryState.id && queryState.name) {
      updateQuery(queryState as SavedQuery);
      Alerter.show({
        message: `Query \u201c${queryState.name}\u201d updated`,
        intent: "success"
      });
    } else {
      saveQuery(query);
      Alerter.show({
        message: `New query saved`,
        intent: "success"
      });
    }
  };

  return (
    <Navbar className="actions">
      <Navbar.Group align={Alignment.LEFT}>
        <Button
          className="actions__execute"
          icon="play"
          intent={Intent.PRIMARY}
          text={loading ? "Executing..." : "Execute"}
          onClick={handleClickExecute}
          disabled={loading}
          title="Execute the current query"
        />
        {queryIsEmpty() ? null : (
          <Button
            className="actions__clear"
            icon="delete"
            text={loading ? "Cancel" : "Clear"}
            onClick={loading ? handleCancelQuery : handleClickClearQuery}
            title="Clear the current query"
          />
        )}
        {queryIsEmpty() ? null : (
          <Button
            className="actions__save"
            icon="floppy-disk"
            text="Save"
            onClick={handleClickSave}
            title="Save the query"
          />
        )}
      </Navbar.Group>
      {executionTime ? (
        <Navbar.Group align={Alignment.RIGHT}>
          <span>
            Query completed in{" "}
            {executionTime.toFixed(0).replace(/\B(?=(\d{3})+(?!\d))/g, ",")} ms.
          </span>
        </Navbar.Group>
      ) : null}
    </Navbar>
  );
}

const mapStateToProps = (state: GlobalState) => ({
    query: state.query,
    result: state.result,
    fhirServer: state.config ? state.config.fhirServer : null
  }),
  actions = {
    fetchQueryResult,
    clearQuery,
    clearResult,
    cancelAndClearResult,
    clearElementTreeFocus,
    saveQuery,
    updateQuery
  };

export default connect(
  mapStateToProps,
  actions
)(Actions);
