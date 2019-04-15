import React from 'react'
import { connect } from 'react-redux'
import { Button, Navbar, Alignment } from '@blueprintjs/core'

import * as actions from '../../store/Actions'

import './Actions.less'

function Actions(props) {
  const { clearQuery } = props

  return (
    <div className="actions">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Button icon="play" text="Execute" minimal={true} />
          <Button
            icon="delete"
            text="Clear query"
            minimal={true}
            onClick={() => clearQuery()}
          />
        </Navbar.Group>
      </Navbar>
    </div>
  )
}

export default connect(
  null,
  actions,
)(Actions)
