import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Toaster, Position } from '@blueprintjs/core'

import ElementTree from '../ElementTree'
import Aggregations from '../Aggregations'
import Filters from '../Filters'
import Groupings from '../Groupings'
import Actions from '../Actions'
import Result from '../Result'
import './App.less'

const Alerter = Toaster.create({
  position: Position.BOTTOM_RIGHT,
})

class App extends Component {
  componentDidCatch(error) {
    Alerter.show({ message: error.message, intent: 'danger' })
    // eslint-disable-next-line no-console
    console.error(error)
  }

  componentDidUpdate(prevProps) {
    const { error } = this.props
    if (error && prevProps.error !== error) {
      Alerter.show({ message: error.get('message'), intent: 'danger' })
      const opOutcome = error.get('opOutcome')
      // eslint-disable-next-line no-console
      if (opOutcome) console.error(opOutcome)
    }
  }

  render() {
    return (
      <div className="app">
        <div className="sider">
          <ElementTree />
        </div>
        <main className="content">
          <Aggregations />
          <Groupings />
          <Filters />
          <Actions />
          <Result />
        </main>
      </div>
    )
  }
}

const mapStateToProps = state => ({ error: state.getIn(['result', 'error']) })
export default connect(mapStateToProps)(App)
