/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React from 'react'
import { ContextMenu, Menu, MenuItem, Icon } from '@blueprintjs/core'

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

/**
 * Opens a context menu at the supplied mouse event which provides actions for
 * adding the specified node to the current query.
 */
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

/**
 * Converts the element tree from the format in the configuration file to an
 * array of objects suitable for provision to the Tree component, via its
 * `contents` prop.
 */
function convertElementTree(tree) {
  const firstPass = Object.keys(tree).map((resourceName, key) => {
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
  return firstPass.map(node => expandReferences(firstPass, node))
}

/**
 * Recursively converts elements within the resource tree, down to a maximum
 * depth defined by the `maxDepth` constant.
 */
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
    } else if (element['type'] === 'Reference') {
      converted.nodeData.elementType = 'Reference'
      converted.nodeData.referenceTypes = element['referenceTypes']
      converted.icon = 'document-share'
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

function expandReferences(fullTree, node) {
  if (node.nodeData.elementType === 'Reference') {
    const referenceTypes = node.nodeData.referenceTypes
    if (referenceTypes[0] === 'Resource') {
      return node
    } else {
      return {
        ...node,
        childNodes: referenceTypes.reduce((acc, t) => {
          const resourceTree = fullTree.find(n => n.id === t)
          return resourceTree ? acc.concat(resourceTree) : acc
        }, []),
      }
    }
  } else {
    if (!node.childNodes) return node
    node.childNodes = node.childNodes.map(childNode =>
      expandReferences(fullTree, childNode),
    )
    return node
  }
}

export default ElementTree
