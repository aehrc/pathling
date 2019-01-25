import React, { Component } from 'react'
import { Table, Button, message } from 'antd'
import { List } from 'immutable'

import './TableResult.less'

class TableResult extends Component {
  static extractData(queryResult) {
    const labelHeadings = this.extractLabelHeadings(queryResult),
      dataHeadings = this.extractDataHeadings(queryResult),
      columns = labelHeadings.concat(dataHeadings).toArray(),
      labelSeries = this.extractLabelSeries(queryResult),
      dataSeries = this.extractDataSeries(queryResult)
    let rowCount = dataSeries.first().size
    if (rowCount >= 1000) rowCount = 1000
    const data = []
    for (let i = 0; i < rowCount; i++) {
      let row = { key: i }
      for (let j = 0; j < labelHeadings.size; j++) {
        const dataIndex = labelHeadings.get(j).dataIndex
        row = { ...row, [dataIndex]: labelSeries.get(j).get(i) }
      }
      for (let k = 0; k < dataHeadings.size; k++) {
        const dataIndex = dataHeadings.get(k).dataIndex
        row = { ...row, [dataIndex]: dataSeries.get(k).get(i) }
      }
      data.push(row)
    }
    return { columns, data }
  }

  static extractLabelHeadings(queryResult) {
    return queryResult.get('label')
      ? queryResult.get('label').map(label => {
          const reference = label.getIn(['dimensionAttribute', 'reference'])
          return {
            title: label.get('name'),
            dataIndex: reference,
            key: reference,
          }
        })
      : List()
  }

  static extractLabelSeries(queryResult) {
    return queryResult.get('label')
      ? queryResult.get('label').map(label =>
          label
            .filter((_, k) => k.match(/^series/))
            .valueSeq()
            .first(),
        )
      : List()
  }

  static extractDataSeries(queryResult) {
    return queryResult.get('data').map(data =>
      data
        .filter((_, k) => k.match(/^series/))
        .valueSeq()
        .first(),
    )
  }

  static extractDataHeadings(queryResult) {
    return queryResult.get('data').map(data => {
      const reference = data.getIn(['metric', 'reference'])
      return {
        title: data.get('name'),
        dataIndex: reference,
        key: reference,
      }
    })
  }

  static sendToClipboard(queryResult) {
    const { columns, data } = TableResult.extractData(queryResult)
    let copyData = columns.map(column => column.title).join('\t') + '\n'
    for (const datum of data) {
      const dataIndices = columns.map(column => column.dataIndex),
        values = []
      for (const dataIndex of dataIndices) {
        values.push(datum[dataIndex])
      }
      copyData += values.join('\t') + '\n'
    }
    return navigator.clipboard
      .writeText(copyData)
      .then(() => message.info('Table copied to clipboard!'))
  }

  componentDidUpdate(prevProps) {
    const { queryResult } = this.props
    if (queryResult.is(prevProps.queryResult)) return
    if (TableResult.extractDataSeries(queryResult).size >= 1000)
      message.warning(
        'More than 1,000 groupings are in this result. These additional data points have been trimmed from the rendering below.',
      )
  }

  render() {
    const { queryResult } = this.props,
      { columns, data } = TableResult.extractData(queryResult)
    return (
      <div className="table-result">
        <Table columns={columns} dataSource={data} pagination={false} />
        <Button
          className="copy-to-clipboard"
          icon="copy"
          onClick={() => TableResult.sendToClipboard(queryResult)}
        />
      </div>
    )
  }
}

export default TableResult
