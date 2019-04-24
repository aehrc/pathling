/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { Component } from 'react'
import { connect } from 'react-redux'
import { Toaster, Position } from '@blueprintjs/core'
import Resizable from 're-resizable'

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

/**
 * Main application component.
 *
 * @author John Grimes
 */
class App extends Component {
  constructor(props) {
    super(props)
    this.state = { siderWidth: 300 }
    this.handleResize = this.handleResize.bind(this)
  }

  /**
   * Catches any uncaught errors that are thrown during the rendering of
   * components.
   */
  componentDidCatch(error) {
    Alerter.show({ message: error.message, intent: 'danger' })
    // eslint-disable-next-line no-console
    console.error(error)
  }

  /**
   * Responds to the update of error details into global state by showing an
   * alert and logging the error to the console.
   */
  componentDidUpdate(prevProps) {
    const { error } = this.props
    if (error && prevProps.error !== error) {
      Alerter.show({ message: error.get('message'), intent: 'danger' })
      const opOutcome = error.get('opOutcome')
      // eslint-disable-next-line no-console
      if (opOutcome) console.error(opOutcome)
    }
  }

  handleResize(event) {
    const { clientX: siderWidth } = event
    if (siderWidth < 200) {
      this.setState(() => ({ siderWidth: 200 }))
    } else if (siderWidth > 600) {
      this.setState(() => ({ siderWidth: 600 }))
    } else {
      this.setState(() => ({ siderWidth }))
    }
  }

  render() {
    const { siderWidth } = this.state

    return (
      <div
        className="app"
        style={{ gridTemplateColumns: `${siderWidth}px auto` }}
      >
        <Resizable
          enable={{
            top: false,
            right: true,
            bottom: false,
            left: false,
            topRight: false,
            bottomRight: false,
            bottomLeft: false,
            topLeft: false,
          }}
          minWidth={200}
          maxWidth={600}
          onResize={this.handleResize}
        >
          <div className="sider">
            <ElementTree />
          </div>
        </Resizable>
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
