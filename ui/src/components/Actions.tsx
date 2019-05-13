/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Button, Navbar, Alignment } from "@blueprintjs/core";

import { fetchQueryResult } from "../store/ResultActions";
import { clearQuery } from "../store/QueryActions";
import { clearElementTreeFocus } from "../store/ElementTreeActions";
import { catchError, clearError } from "../store/ErrorActions";
import { QueryState } from "../store/QueryReducer";
import { ResultState } from "../store/ResultReducer";
import { GlobalState } from "../store";
import "./style/Actions.scss";

interface Props {
  query: QueryState;
  result: ResultState;
  fhirServer?: string;
  fetchQueryResult?: (fhirServer: string) => any;
  clearQuery?: () => any;
  catchError?: (message: string) => any;
  clearElementTreeFocus?: () => any;
  clearError?: () => any;
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
    clearElementTreeFocus,
    clearError,
    catchError,
    query,
    result: { loading },
    fhirServer
  } = props;

  const queryIsEmpty = (): boolean =>
    query.aggregations.length === 0 && query.groupings.length === 0;

  const handleClickExecute = () => {
    if (!fhirServer) {
      catchError("Missing FHIR server configuration value");
    } else {
      fetchQueryResult(fhirServer);
    }
  };

  const handleClickClearQuery = () => {
    clearQuery();
    clearElementTreeFocus();
    clearError();
  };

  return (
    <Navbar className="actions">
      <Navbar.Group align={Alignment.LEFT}>
        <Button
          className="execute"
          icon="play"
          text={loading ? "Executing..." : "Execute"}
          minimal={true}
          onClick={handleClickExecute}
          disabled={loading}
        />
        {queryIsEmpty() ? null : (
          <Button
            className="clear"
            icon="delete"
            text="Clear query"
            minimal={true}
            onClick={handleClickClearQuery}
          />
        )}
      </Navbar.Group>
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
    clearElementTreeFocus,
    catchError,
    clearError
  };

export default connect(
  mapStateToProps,
  actions
)(Actions);
