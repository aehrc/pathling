/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'
import { connect } from 'react-redux'
import { Tree, ContextMenu, Menu, MenuItem } from '@blueprintjs/core'
import { fromJS, Map, List } from 'immutable'

import * as actions from '../../store/Actions'
import './ElementTree.less'
import elementTree from '../../../config/element-tree.json'

/**
 * Renders a tree showing resources and elements available for use within
 * analytic queries.
 *
 * @author John Grimes
 */
function ElementTree(props) {
  const { addAggregation, addGrouping } = props
  const treeForConversion = fromJS(elementTree)
  const [tree, setTree] = useState(convertElementTree(treeForConversion))

  function handleNodeExpand(_, nodePath) {
    return updateIsExpanded(nodePath, true)
  }

  function handleNodeCollapse(_, nodePath) {
    return updateIsExpanded(nodePath, false)
  }

  function handleNodeClick(node, nodePath) {
    const elementType = node.nodeData.elementType,
      expandable =
        elementType === 'Resource' || elementType === 'BackboneElement'
    if (expandable && !node.isExpanded) handleNodeExpand(node, nodePath)
    if (expandable && node.isExpanded) handleNodeCollapse(node, nodePath)
  }

  function handleNodeContextMenu(node, path, event) {
    const aggregationExpression = `${node.nodeData.fhirPath}.count()`,
      aggregationLabel = aggregationExpression,
      groupingExpression = node.nodeData.fhirPath,
      groupingLabel = groupingExpression,
      aggregationMenuItem = (
        <MenuItem
          icon="trending-up"
          text={`Add "${aggregationExpression}" to aggregations`}
          onClick={() =>
            addAggregation({
              expression: aggregationExpression,
              label: aggregationLabel,
            })
          }
        />
      ),
      groupingMenuItem = (
        <MenuItem
          icon="graph"
          text={`Add "${groupingExpression}" to groupings`}
          onClick={() =>
            addGrouping({
              expression: groupingExpression,
              label: groupingLabel,
            })
          }
        />
      )
    event.preventDefault()
    if (node.nodeData.elementType === 'Element') {
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

  function updateIsExpanded(nodePath, value) {
    const updatePath = nodePath
      .reduce(
        (acc, x, n) =>
          n !== 0 ? acc.concat(['childNodes', x]) : acc.concat(x),
        [],
      )
      .concat('isExpanded')
    const updatedTree = List(tree).updateIn(updatePath, () => value)
    setTree(updatedTree)
  }

  function convertElementTree(tree) {
    return tree.keySeq().map((resourceName, key) =>
      Map({
        id: resourceName,
        label: resourceName,
        childNodes: convertElements(tree.get(resourceName), 1, [key]),
        depth: 1,
        path: [key],
        icon: 'cube',
        nodeData: Map({ fhirPath: resourceName, elementType: 'Resource' }),
      }),
    )
  }

  function convertElements(elements, depth, path) {
    if (!elements) return null
    return elements.map((element, key) => {
      const newDepth = depth + 1
      const newPath = path.concat(key)
      let converted = Map({
        id: element.get('name'),
        label: element.get('name'),
        depth: newDepth,
        path: newPath,
        icon: 'property',
        nodeData: Map({ fhirPath: element.get('path') }),
      })
      if (element.get('children')) {
        converted = converted.set(
          'childNodes',
          convertElements(element.get('children'), newDepth, newPath),
        )
        converted = converted.set('icon', 'folder-close')
        converted = converted.setIn(
          ['nodeData', 'elementType'],
          'BackboneElement',
        )
      } else {
        converted = converted.setIn(['nodeData', 'elementType'], 'Element')
      }
      return converted
    })
  }

  return (
    <Tree
      className="element-tree"
      contents={tree.toJS()}
      onNodeClick={handleNodeClick}
      onNodeExpand={handleNodeExpand}
      onNodeCollapse={handleNodeCollapse}
      onNodeContextMenu={handleNodeContextMenu}
    />
  )
}

export default connect(
  null,
  actions,
)(ElementTree)
