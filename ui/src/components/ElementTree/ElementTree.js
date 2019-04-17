/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState, useReducer } from 'react'
import { connect } from 'react-redux'
import { Tree, ContextMenu, Menu, MenuItem, Icon } from '@blueprintjs/core'

import store from '../../store'
import { addAggregation, addGrouping } from '../../store/Actions'
import './ElementTree.less'
import resourceTree from '../../../config/resource-tree.json'
import complexTypesTree from '../../../config/complex-type-tree.json'

const maxDepth = 10
const supportedComplexTypes = [
  'Ratio',
  'Period',
  'Range',
  'Attachment',
  'Identifier',
  'HumanName',
  'Annotation',
  'Address',
  'ContactPoint',
  'SampledData',
  'Money',
  'Count',
  'Duration',
  'SimpleQuantity',
  'Quantity',
  'Distance',
  'Age',
  'CodeableConcept',
  'Signature',
  'Coding',
  'Timing',
  'Reference',
]
const convertedResourceTree = convertElementTree(resourceTree)

/**
 * Renders a tree showing resources and elements available for use within
 * analytic queries.
 *
 * @author John Grimes
 */
function ElementTree() {
  const [tree, setTree] = useState(convertedResourceTree)
  const [update, forceUpdate] = useReducer(x => x + 1, 0)

  function handleNodeExpand(node) {
    node.isExpanded = true
    forceUpdate()
  }

  function handleNodeCollapse(node) {
    node.isExpanded = false
    forceUpdate()
  }

  console.log('render')
  return (
    <Tree
      className="element-tree"
      contents={tree}
      onNodeExpand={handleNodeExpand}
      onNodeCollapse={handleNodeCollapse}
    />
  )
}

function openContextMenu(event, nodeData) {
  const aggregationExpression = `${nodeData.fhirPath}.count()`,
    aggregationLabel = aggregationExpression,
    groupingExpression = nodeData.fhirPath,
    groupingLabel = groupingExpression,
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
    ),
    groupingMenuItem = (
      <MenuItem
        icon="graph"
        text={`Add "${groupingExpression}" to groupings`}
        onClick={() =>
          store.dispatch(
            addGrouping({
              expression: groupingExpression,
              label: groupingLabel,
            }),
          )
        }
      />
    )
  if (nodeData.elementType === 'Element') {
    ContextMenu.show(
      <Menu>
        {aggregationMenuItem}
        {groupingMenuItem}
      </Menu>,
      { left: event.clientX, top: event.clientY },
    )
  } else {
    ContextMenu.show(<Menu>{aggregationMenuItem}</Menu>, {
      left: event.clientX,
      top: event.clientY,
    })
  }
}

function convertElementTree(tree) {
  console.log('convertElementTree')
  return Object.keys(tree).map((resourceName, key) => {
    const nodeData = { fhirPath: resourceName, elementType: 'Resource' }
    return {
      id: resourceName,
      label: resourceName,
      childNodes: convertElements(tree[resourceName], 1, [key], resourceName),
      depth: 1,
      path: [key],
      icon: 'cube',
      nodeData,
      secondaryLabel: (
        <Icon icon="add" onClick={event => openContextMenu(event, nodeData)} />
      ),
    }
  })
}

function convertElements(elements, depth, path, fhirPath) {
  if (!elements || depth > maxDepth) return null
  return elements.map((element, key) => {
    const newDepth = depth + 1
    const newPath = path.concat(key)
    const pathTokens = element['path'].split('.')
    const newFhirPath = fhirPath + '.' + pathTokens[pathTokens.length - 1]
    let converted = {
      id: element['name'],
      label: element['name'],
      depth: newDepth,
      path: newPath,
      icon: 'symbol-square',
      nodeData: { fhirPath: newFhirPath },
    }
    if (element['children']) {
      converted.childNodes = convertElements(
        element['children'],
        newDepth,
        newPath,
        newFhirPath,
      )
      converted.icon = 'folder-close'
      converted.nodeData.elementType = 'BackboneElement'
    } else if (supportedComplexTypes.includes(element['type'])) {
      converted.childNodes = convertElements(
        complexTypesTree[element['type']],
        newDepth,
        newPath,
        newFhirPath,
      )
      converted.nodeData.elementType = element['type']
      converted.icon = 'grid-view'
    } else {
      converted.nodeData.elementType = 'Element'
      converted.secondaryLabel = (
        <Icon
          icon="add"
          onClick={event => openContextMenu(event, converted.nodeData)}
        />
      )
    }
    return converted
  })
}

export default ElementTree
