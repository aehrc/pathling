/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'
import { Icon } from '@blueprintjs/core'

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

  function renderBackboneElementChildren() {
    const elementTreeNodes = backboneElementChildren.map((node, i) => (
      <ElementTreeNode
        {...node.delete('children').toJS()}
        key={i}
        path={path.concat('children', i)}
        resourceOrComplexType={resourceOrComplexType}
      />
    ))
    return <ul className="child-nodes">{elementTreeNodes}</ul>
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
    return <ul className="child-nodes">{elementTreeNodes}</ul>
  }

  function renderReferenceChildren() {
    const resourceTreeNodes = referenceChildren.map((type, i) => (
      <ResourceTreeNode name={type} key={i} />
    ))
    return <ul className="child-nodes">{resourceTreeNodes}</ul>
  }

  return (
    <li className="element-tree-node">
      {backboneElementChildren ||
      complexElementChildren ||
      (referenceChildren && referenceChildren.length > 0) ? (
        <Icon
          icon={isExpanded ? 'chevron-down' : 'chevron-up'}
          onClick={() => setExpanded(!isExpanded)}
        />
      ) : null}
      <div className="name">{name}</div>
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
