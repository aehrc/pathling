/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { Icon } from '@blueprintjs/core'
import './Filters.less'

/**
 * Renders a list of currently selected filters, used when composing a query.
 *
 * @author John Grimes
 */
function Filters() {
  return (
    <div className="filters">
      <div className="blank-canvas">
        <Icon icon="filter" />
        <span>Filters</span>
      </div>
    </div>
  )
}

export default Filters
