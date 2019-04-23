/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'
import { MenuItem, Menu, ContextMenu } from '@blueprintjs/core'

import store from '../../store'
import { addAggregation, addGrouping } from '../../store/Actions'

import ElementTreeNode from '../ElementTreeNode'
import { resourceTree } from '../../fhir/ResourceTree'

function ResourceTreeNode(props) {
  const { name } = props,
    [isExpanded, setExpanded] = useState(false)

  function openContextMenu(event) {
    const aggregationExpression = `${name}.count()`,
      aggregationLabel = aggregationExpression,
      aggregationMenuItem = (
        <MenuItem
          icon="trending-up"
          text={`Add "${aggregationExpression}" to aggregations`}
          onClick={() =>
            store.dispatch(
              addAggregation({
                expression: aggregationExpression,
                label: aggregationLabel,
              }),
            )
          }
        />
      )

    ContextMenu.show(<Menu>{aggregationMenuItem}</Menu>, {
      left: event.clientX,
      top: event.clientY,
    })
  }

  function renderChildren() {
    const childNodes = resourceTree.get(name),
      elementTreeNodes = childNodes.map((node, i) => (
        <ElementTreeNode
          {...node.delete('children').toJS()}
          key={i}
          treePath={[name, i]}
          resourceOrComplexType={name}
        />
      ))
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    )
  }

  function renderActionIcon() {
    return (
      <span
        className="bp3-tree-node-secondary-label bp3-icon-standard bp3-icon-arrow-right"
        onClick={openContextMenu}
      />
    )
  }

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

  return (
    <li className={getNodeClasses()}>
      <div className="bp3-tree-node-content">
        <span
          className={getCaretClasses()}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className="bp3-tree-node-icon bp3-icon-standard bp3-icon-cube" />
        <span className="name bp3-tree-node-label" onClick={openContextMenu}>
          {name}
        </span>
        {renderActionIcon()}
      </div>
      {isExpanded ? renderChildren() : null}
    </li>
  )
}

export default ResourceTreeNode
