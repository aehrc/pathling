/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { connect } from "react-redux";
import { Button, Navbar, Alignment } from "@blueprintjs/core";

import * as actions from "../store/ResultActions";
import { Query } from "../store/QueryReducer";
import { Result } from "../store/ResultReducer";
import "./Actions.scss";
import { GlobalState } from "../store";

interface Props {
  query: Query;
  result: Result;
  fetchQueryResult?: () => void;
  clearQuery?: () => void;
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
    query,
    result: { loading }
  } = props;

  const queryIsEmpty = (): boolean =>
    query.aggregations.length === 0 && query.groupings.length === 0;

  return (
    <div className="actions">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Button
            icon="play"
            text={loading ? "Executing..." : "Execute"}
            minimal={true}
            onClick={fetchQueryResult}
            disabled={loading}
          />
          {queryIsEmpty() ? null : (
            <Button
              icon="delete"
              text="Clear query"
              minimal={true}
              onClick={clearQuery}
            />
          )}
        </Navbar.Group>
      </Navbar>
    </div>
  );
}

const mapStateToProps = (state: GlobalState) => ({
  query: state.query,
  result: state.result
});

export default connect(
  mapStateToProps,
  actions
)(Actions);
