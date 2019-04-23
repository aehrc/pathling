/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'

import {
  resourceTree,
  complexTypesTree,
  supportedComplexTypes,
} from '../../fhir/ResourceTree'
import ResourceTreeNode from '../ResourceTreeNode'

function ElementTreeNode(props) {
  const { name, path, type, resourceOrComplexType, referenceTypes } = props,
    [isExpanded, setExpanded] = useState(false),
    isInResourceTree = !!resourceTree.get(resourceOrComplexType),
    backboneElementChildren = isInResourceTree
      ? resourceTree.getIn(path).get('children')
      : null,
    isComplexType = supportedComplexTypes.includes(type),
    complexElementChildren = isComplexType ? complexTypesTree.get(type) : null,
    isReference = type === 'Reference',
    referenceChildren = isReference
      ? referenceTypes.filter(t => !!resourceTree.get(t))
      : null

  function getCaretClasses() {
    if (
      backboneElementChildren ||
      complexElementChildren ||
      (referenceChildren && referenceChildren.length > 0)
    ) {
      return isExpanded
        ? 'bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard'
        : 'bp3-tree-node-caret bp3-tree-node-caret-close bp3-icon-standard'
    } else {
      return 'bp3-tree-node-caret-none bp3-icon-standard'
    }
  }

  function getIconName() {
    if (isComplexType) {
      return 'grid-view'
    } else if (isReference) {
      return 'document-share'
    } else if (isInResourceTree) {
      return 'symbol-square'
    }
  }

  function renderBackboneElementChildren() {
    const elementTreeNodes = backboneElementChildren.map((node, i) => (
      <ElementTreeNode
        {...node.delete('children').toJS()}
        key={i}
        path={path.concat('children', i)}
        resourceOrComplexType={resourceOrComplexType}
      />
    ))
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    )
  }

  function renderComplexElementChildren() {
    const elementTreeNodes = complexElementChildren.map((node, i) => (
      <ElementTreeNode
        {...node.delete('children').toJS()}
        key={i}
        path={isComplexType ? [type, i] : path.concat('children', i)}
        resourceOrComplexType={isComplexType ? type : resourceOrComplexType}
      />
    ))
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    )
  }

  function renderReferenceChildren() {
    const resourceTreeNodes = referenceChildren.map((type, i) => (
      <ResourceTreeNode name={type} key={i} />
    ))
    return (
      <ol className="child-nodes bp3-tree-node-list">{resourceTreeNodes}</ol>
    )
  }

  return (
    <li className="element-tree-node bp3-tree-node">
      <div className="bp3-tree-node-content">
        <span
          className={getCaretClasses()}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span
          className={`bp3-tree-node-icon bp3-icon-standard bp3-icon-${getIconName()}`}
        />
        <span className="name bp3-tree-node-label">{name}</span>
      </div>
      {isExpanded && backboneElementChildren
        ? renderBackboneElementChildren()
        : null}
      {isExpanded && complexElementChildren
        ? renderComplexElementChildren()
        : null}
      {isExpanded && referenceChildren ? renderReferenceChildren() : null}
    </li>
  )
}

export default ElementTreeNode
