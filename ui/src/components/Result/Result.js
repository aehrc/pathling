/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { connect } from 'react-redux'
import { Spinner } from '@blueprintjs/core'

import './Result.less'

function Result(props) {
  const { loading } = props
  return (
    <div className="result">
      {loading ? (
        <Spinner className="loading" size={100} intent="primary" />
      ) : null}
    </div>
  )
}

const mapStateToProps = state => ({
  loading: state.getIn(['result', 'loading']),
})
export default connect(mapStateToProps)(Result)
