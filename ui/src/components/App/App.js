import React from 'react'

import ElementTree from '../ElementTree'
import Aggregations from '../Aggregations'
import Filters from '../Filters'
import Groupings from '../Groupings'
import Actions from '../Actions'
import Result from '../Result'
import './App.less'

const App = () => {
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

export default App
