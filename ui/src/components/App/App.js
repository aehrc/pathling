import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Icon, Spin, message } from 'antd'

import ElementTree from '../ElementTree'
import DimensionList from '../DimensionList'
import MetricList from '../MetricList'
import SelectedMetrics from '../SelectedMetrics'
import SelectedDimensionAttributes from '../SelectedDimensionAttributes'
import TableResult from '../TableResult'
import './App.less'

class App extends Component {
  componentDidCatch(error) {
    message.error(error.message)
  }

  componentDidUpdate(prevProps) {
    if (prevProps.error !== this.props.error) {
      message.error(this.props.error.get('message'))
      const opOutcome = this.props.error.get('opOutcome')
      if (opOutcome) console.error(opOutcome)
    }
  }

  render() {
    return (
      <div className="app">
        <menu className="sider">
          <ElementTree />
          <MetricList />
          <DimensionList />
        </menu>
        <main className="content">
          <SelectedMetrics />
          <SelectedDimensionAttributes />
          <div className="filters">
            <Icon type="filter" />
            <span>Filters</span>
          </div>
          <div className="content-inner">{this.renderResult()}</div>
        </main>
      </div>
    )
  }

  renderResult() {
    const { queryResult, loading } = this.props
    if (loading)
      return (
        <Spin
          className="loading"
          delay={50}
          indicator={<Icon type="loading" style={{ fontSize: 24 }} spin />}
        />
      )
    return queryResult ? (
      <TableResult queryResult={queryResult} />
    ) : (
      'Query Result'
    )
  }
}

const mapStateToProps = state => ({
  queryResult: state.getIn(['query', 'queryResult']),
  loading: state.getIn(['query', 'loading']),
  error: state.getIn(['app', 'error']),
})

export default connect(mapStateToProps)(App)
