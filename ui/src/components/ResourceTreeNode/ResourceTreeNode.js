/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'
import { Icon } from '@blueprintjs/core'

import ElementTreeNode from '../ElementTreeNode'
import { resourceTree } from '../../fhir/ResourceTree'

function ResourceTreeNode(props) {
  const { name } = props
  const [isExpanded, setExpanded] = useState(false)

  function renderChildren() {
    const childNodes = resourceTree.get(name),
      elementTreeNodes = childNodes.map((node, i) => (
        <ElementTreeNode
          {...node.delete('children').toJS()}
          key={i}
          path={[name, i]}
          resourceOrComplexType={name}
        />
      ))
    return <ul className="child-nodes">{elementTreeNodes}</ul>
  }

  return (
    <li className="resource-tree-node">
      <Icon
        icon={isExpanded ? 'chevron-down' : 'chevron-up'}
        onClick={() => setExpanded(!isExpanded)}
      />
      <div className="name">{name}</div>
      {isExpanded ? renderChildren() : null}
    </li>
  )
}

export default ResourceTreeNode
