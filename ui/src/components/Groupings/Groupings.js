/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { connect } from 'react-redux'
import { Icon, Tag } from '@blueprintjs/core'

import * as actions from '../../store/Actions'
import './Groupings.less'

/**
 * Renders a list of currently selected groupings, used when composing a query.
 *
 * @author John Grimes
 */
function Groupings(props) {
  const { groupings, removeGrouping } = props

  function handleRemove(index) {
    removeGrouping(index)
  }

  function renderBlankCanvas() {
    return <div className="blank-canvas">Groupings</div>
  }

  function renderGroupings() {
    return groupings.map((grouping, i) => (
      <Tag key={i} round={true} large={true} onRemove={() => handleRemove(i)}>
        {grouping.get('label')}
      </Tag>
    ))
  }

  return (
    <div className="groupings">
      <Icon className="section-identity" icon="graph" />
      {groupings.isEmpty() ? renderBlankCanvas() : renderGroupings(groupings)}
    </div>
  )
}

const mapStateToProps = state => ({
  groupings: state.getIn(['query', 'groupings']),
})
export default connect(
  mapStateToProps,
  actions,
)(Groupings)
