import React from 'react'
import { connect } from 'react-redux'
import { Button, Navbar, Alignment } from '@blueprintjs/core'

import * as actions from '../../store/Actions'

import './Actions.less'

function Actions(props) {
  const { fetchQueryResult, clearQuery, query } = props

  function queryIsEmpty() {
    return query.aggregations.length === 0 && query.groupings.length === 0
  }

  return (
    <div className="actions">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Button
            icon="play"
            text="Execute"
            minimal={true}
            onClick={fetchQueryResult}
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

const mapStateToProps = state => ({ query: state.get('query').toJS() })
export default connect(
  mapStateToProps,
  actions,
)(Actions)
