import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Menu, Tag, Icon } from 'antd'
import { Map } from 'immutable'

import * as actionCreators from '../../store/Actions'
import './MetricList.less'

class MetricList extends Component {
  handleSelectMetric(metric) {
    const { selectMetric, fetchQueryResult } = this.props,
      reference = Map({
        reference: `Metric/${metric.get('id')}`,
        display: metric.get('name'),
      })
    selectMetric(reference)
    fetchQueryResult()
  }

  componentDidMount() {
    const { fetchMetrics } = this.props
    fetchMetrics()
  }

  render() {
    const { metrics, loading } = this.props,
      iconType = loading ? 'loading' : 'dashboard'
    return (
      <Menu mode="inline" defaultOpenKeys={['metrics']}>
        <Menu.SubMenu
          key="metrics"
          className="metric-list"
          title={
            <span>
              <Icon
                className="metric-list-icon"
                type={iconType}
                theme="filled"
              />
              <span>Metrics</span>
            </span>
          }
        >
          <React.Fragment>
            {metrics ? this.renderMetrics(metrics) : null}
          </React.Fragment>
        </Menu.SubMenu>
      </Menu>
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
              title={metric.get('name')}
              onClick={() => this.handleSelectMetric(metric)}
            >
              <span>{metric.get('name')}</span>
              <Icon type="plus" />
            </Tag>
          )
        })}
      </div>
    )
  }
}

const mapStateToProps = state => ({
  metrics: state.getIn(['metrics', 'metrics']),
  loading: state.getIn(['metrics', 'loading']),
})

export default connect(
  mapStateToProps,
  actionCreators,
)(MetricList)
