import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Icon, Tag } from 'antd'

import * as actionCreators from '../../store/Actions'
import './SelectedMetrics.less'

class SelectedMetrics extends Component {
  handleDeselectMetric(metric) {
    const { deselectMetric, fetchQueryResult } = this.props
    deselectMetric(metric)
    fetchQueryResult()
  }

  render() {
    const { selectedMetrics } = this.props
    return (
      <div className="selected-metrics">
        {selectedMetrics.size > 0 ? (
          this.renderMetrics(selectedMetrics)
        ) : (
          <React.Fragment>
            <Icon type="dashboard" />
            <span>Metrics</span>
          </React.Fragment>
        )}
      </div>
    )
  }

  renderMetrics(metrics) {
    return (
      <div className="metrics">
        {metrics.map((metric, i) => {
          return (
            <Tag
              key={i}
              className="metric"
              color="#14A01E"
              title={metric.get('display')}
              onClick={() => this.handleDeselectMetric(metric)}
            >
              <span>{metric.get('display')}</span>
              <Icon type="close" />
            </Tag>
          )
        })}
      </div>
    )
  }
}

const mapStateToProps = state => ({
  selectedMetrics: state.getIn(['metrics', 'selectedMetrics']),
})

export default connect(
  mapStateToProps,
  actionCreators,
)(SelectedMetrics)
