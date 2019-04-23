/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { connect } from 'react-redux'
import { Button, Navbar, Alignment } from '@blueprintjs/core'

import * as actions from '../../store/Actions'

import './Actions.less'

/**
 * Renders a toolbar containing actions relating to the currently entered query.
 *
 * @author John Grimes
 */
function Actions(props) {
  const {
    fetchQueryResult,
    clearQuery,
    query,
    result: { loading },
  } = props

  const queryIsEmpty = () =>
    query.aggregations.length === 0 && query.groupings.length === 0

  return (
    <div className="actions">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Button
            icon="play"
            text={loading ? 'Executing...' : 'Execute'}
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
  )
}

const mapStateToProps = state =>
  state.filter((_, k) => ['query', 'result'].includes(k)).toJS()
export default connect(
  mapStateToProps,
  actions,
)(Actions)
