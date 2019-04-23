/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { connect } from 'react-redux'
import { Icon, Tag } from '@blueprintjs/core'

import * as actions from '../../store/Actions'
import './Aggregations.less'

/**
 * Renders a control which can be used to represent a set of selected
 * aggregations, when composing a query.
 *
 * @author John Grimes
 */
function Aggregations(props) {
  const { aggregations, removeAggregation } = props

  const handleRemove = index => {
    removeAggregation(index)
  }

  const renderBlankCanvas = () => (
    <div className="blank-canvas">Aggregations</div>
  )

  const renderAggregations = () =>
    aggregations.map((aggregation, i) => (
      <Tag key={i} round={true} large={true} onRemove={() => handleRemove(i)}>
        {aggregation.get('label')}
      </Tag>
    ))

  return (
    <div className="aggregations">
      <Icon className="section-identity" icon="trending-up" />
      {aggregations.isEmpty()
        ? renderBlankCanvas()
        : renderAggregations(aggregations)}
    </div>
  )
}

const mapStateToProps = state => ({
  aggregations: state.getIn(['query', 'aggregations']),
})
export default connect(
  mapStateToProps,
  actions,
)(Aggregations)
