import React from 'react'
import { Icon } from '@blueprintjs/core'
import './Filters.less'

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
