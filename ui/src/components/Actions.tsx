/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { useState } from "react";
import { connect } from "react-redux";
import { Alignment, Button, Intent, Navbar } from "@blueprintjs/core";

import {
  cancelAndClearResult,
  clearResult,
  fetchQueryResult
} from "../store/ResultActions";
import { clearQuery } from "../store/QueryActions";
import { clearElementTreeFocus } from "../store/ElementTreeActions";
import { catchError, clearError } from "../store/ErrorActions";
import { QueryState } from "../store/QueryReducer";
import { ResultState } from "../store/ResultReducer";
import { GlobalState } from "../store";
import "./style/Actions.scss";
import SaveDialog from "./SaveDialog";
import { SavedQuery } from "../store/SavedQueriesReducer";
import { saveQuery } from "../store/SavedQueriesActions";

interface Props {
  query: QueryState;
  result: ResultState;
  fhirServer?: string;
  fetchQueryResult?: (fhirServer: string) => any;
  clearQuery?: () => any;
  clearResult?: () => any;
  cancelAndClearResult?: () => any;
  catchError?: (message: string) => any;
  clearElementTreeFocus?: () => any;
  clearError?: () => any;
  saveQuery?: (name: string, query: SavedQuery) => any;
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
      clearError,
      catchError,
      saveQuery,
      query,
      result: { loading, executionTime },
      fhirServer
    } = props,
    [saveDialogIsOpen, setSaveDialogOpen] = useState(false);

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
    clearResult();
    clearElementTreeFocus();
    clearError();
  };

  const handleCancelQuery = () => {
    cancelAndClearResult();
    clearError();
  };

  const handleClickSave = () => {
    setSaveDialogOpen(true);
  };

  const handleCloseSaveDialog = () => {
    setSaveDialogOpen(false);
  };

  const handleSave = (name: string) => {
    saveQuery(name, query);
    setSaveDialogOpen(false);
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
        />
        {queryIsEmpty() ? null : (
          <Button
            className="actions__clear"
            icon="delete"
            text={loading ? "Cancel" : "Clear"}
            onClick={loading ? handleCancelQuery : handleClickClearQuery}
          />
        )}
        {queryIsEmpty() ? null : (
          <Button
            className="actions__save"
            icon="floppy-disk"
            text="Save"
            onClick={handleClickSave}
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
      <SaveDialog
        isOpen={saveDialogIsOpen}
        onClose={handleCloseSaveDialog}
        onSave={handleSave}
      />
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
    catchError,
    clearError,
    saveQuery
  };

export default connect(
  mapStateToProps,
  actions
)(Actions);
