import React from 'react'
import { connect } from 'react-redux'
import { Icon, Tag } from '@blueprintjs/core'

import * as actions from '../../store/Actions'
import './Aggregations.less'

function Aggregations(props) {
  const { aggregations, removeAggregation } = props

  function handleRemove(index) {
    removeAggregation(index)
  }

  function renderBlankCanvas() {
    return <span className="blank-canvas">Aggregations</span>
  }

  function renderAggregations() {
    return aggregations.map((aggregation, i) => (
      <Tag key={i} round={true} large={true} onRemove={() => handleRemove(i)}>
        {aggregation.get('label')}
      </Tag>
    ))
  }

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
