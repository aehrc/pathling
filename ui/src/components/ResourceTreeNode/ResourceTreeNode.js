/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'

import ElementTreeNode from '../ElementTreeNode'
import { resourceTree } from '../../fhir/ResourceTree'

function ResourceTreeNode(props) {
  const { name } = props,
    [isExpanded, setExpanded] = useState(false)

  function getNodeClasses() {
    return isExpanded
      ? 'resource-tree-node bp3-tree-node bp3-tree-node-expanded'
      : 'resource-tree-node bp3-tree-node'
  }

  function getCaretClasses() {
    return isExpanded
      ? 'bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard'
      : 'bp3-tree-node-caret bp3-tree-node-caret-close bp3-icon-standard'
  }

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
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    )
  }

  return (
    <li className={getNodeClasses()}>
      <div className="bp3-tree-node-content">
        <span
          className={getCaretClasses()}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className="bp3-tree-node-icon bp3-icon-standard bp3-icon-cube" />
        <span className="name bp3-tree-node-label">{name}</span>
      </div>
      {isExpanded ? renderChildren() : null}
    </li>
  )
}

export default ResourceTreeNode
