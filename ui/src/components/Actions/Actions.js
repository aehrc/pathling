import { Button, Navbar, Alignment } from '@blueprintjs/core'
import React from 'react'

import './Actions.less'

function Actions() {
  return (
    <div className="actions">
      <Navbar>
        <Navbar.Group align={Alignment.LEFT}>
          <Button icon="play" text="Execute" minimal={true} />
          <Button icon="delete" text="Clear query" minimal={true} />
        </Navbar.Group>
      </Navbar>
    </div>
  )
}

export default Actions
