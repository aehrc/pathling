/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'

import ResourceTreeNode from '../ResourceTreeNode'
import { resourceTree } from '../../fhir/ResourceTree'
import './ElementTree.less'

/**
 * Renders a tree showing resources and elements available for use within
 * analytic queries.
 *
 * @author John Grimes
 */
function ElementTree() {
  const resourceNodes = resourceTree
    .keySeq()
    .map(resourceName => (
      <ResourceTreeNode key={resourceName} name={resourceName} />
    ))
    .toArray()

  return (
    <div className="element-tree bp3-tree">
      <ol className="bp3-tree-node-list bp3-tree-root">{resourceNodes}</ol>
    </div>
  )
}

export default ElementTree
