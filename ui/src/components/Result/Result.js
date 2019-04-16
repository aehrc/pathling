/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { connect } from 'react-redux'
import { Spinner, HTMLTable } from '@blueprintjs/core'

import './Result.less'

function Result(props) {
  const { loading, groupings, query, stale } = props

  function renderLoading() {
    return <Spinner className="loading" size={100} intent="primary" />
  }

  function renderGroupings() {
    const groupHeadings = query.groupings.map(grouping => (
        <th key={grouping.label}>{grouping.label}</th>
      )),
      aggregationHeadings = query.aggregations.map(aggregation => (
        <th key={aggregation.label}>{aggregation.label}</th>
      )),
      rows = groupings.map((grouping, i) => renderGrouping(grouping, i))
    return (
      <HTMLTable interactive={true}>
        <thead>
          <tr>{groupHeadings.concat(aggregationHeadings)}</tr>
        </thead>
        <tbody>{rows}</tbody>
      </HTMLTable>
    )
  }

  function renderGrouping(grouping, i) {
    const parts = grouping.part.map((part, i) => renderPart(part, i))
    return <tr key={i}>{parts}</tr>
  }

  function renderPart(part, i) {
    const key = Object.keys(part).find(key => key.match(/^value/)),
      value = part[key]
    if (value === undefined) {
      return <td key={i}>(no value)</td>
    } else if (key === 'valueDate') {
      const date = new Date(value).toLocaleDateString()
      return <td key={i}>{date}</td>
    } else if (key === 'valueDateTime') {
      const date = new Date(value).toLocaleString()
      return <td key={i}>{date}</td>
    } else {
      return <td key={i}>{part[key].toString()}</td>
    }
  }

  let content = null
  if (loading) {
    content = renderLoading()
  } else if (groupings !== null) {
    content = renderGroupings()
  }
  return <div className={stale ? 'result stale' : 'result'}>{content}</div>
}

function checkStale(state) {
  return !(
    state.getIn(['result', 'query']) !== null &&
    state.get('query').hashCode() ===
      state.getIn(['result', 'query']).hashCode()
  )
}

const mapStateToProps = state =>
  state
    .get('result')
    .merge({ stale: checkStale(state) })
    .toJS()

export default connect(mapStateToProps)(Result)
