/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from 'react'
import { connect } from 'react-redux'
import {
  MenuItem,
  Menu,
  ContextMenu,
  Popover,
  PopoverInteractionKind,
  Position,
} from '@blueprintjs/core'

import ElementTreeNode from '../ElementTreeNode'
import { resourceTree, reverseReferences } from '../../fhir/ResourceTree'
import * as actions from '../../store/Actions'
import './ResourceTreeNode.less'

export const ResourceTreeNodeType = {
  RESOURCE: 'RESOURCE',
  REFERENCE: 'REFERENCE',
  REVERSE_REFERENCE: 'REVERSE_REFERENCE',
}

/**
 * Renders a tree node showing the elements available within a particular
 * resource type.
 *
 * @author John Grimes
 */
function ResourceTreeNode(props) {
  const { name, type, resourceType, referencePath, addAggregation } = props,
    label = props.label ? props.label : name,
    children = !!resourceTree.get(name),
    [isExpanded, setExpanded] = useState(false)

  const openContextMenu = event => {
    const aggregationExpression = `${name}.count()`,
      aggregationLabel = aggregationExpression,
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
      )

    ContextMenu.show(<Menu>{aggregationMenuItem}</Menu>, {
      left: event.clientX,
      top: event.clientY,
    })
  }

  const getReferencePath = node => {
    if (!referencePath) return node.get('path')
    const pathSuffix = node
      .get('path')
      .split('.')
      .slice(1)
      .join('.')
    return `${referencePath}.${pathSuffix}`
  }

  const getDefinition = () => {
    if (props.definition) {
      return props.definition
    } else {
      return children ? resourceTree.get(name).get('definition') : null
    }
  }

  const getIconClasses = () => {
    let iconName = null
    if (type === ResourceTreeNodeType.REFERENCE) {
      iconName = 'document-share'
    } else if (type === ResourceTreeNodeType.REVERSE_REFERENCE) {
      iconName = 'document-open'
    } else {
      iconName = 'cube'
    }
    return `bp3-tree-node-icon bp3-icon-standard bp3-icon-${iconName}`
  }

  const getNodeClasses = () => {
    let modifier = ''
    if (type === ResourceTreeNodeType.REFERENCE) {
      modifier = 'resource-tree-node-reference'
    } else if (type === ResourceTreeNodeType.REVERSE_REFERENCE) {
      modifier = 'resource-tree-node-reverse-reference'
    }
    return isExpanded
      ? `resource-tree-node ${modifier}bp3-tree-node bp3-tree-node-expanded`
      : `resource-tree-node ${modifier}bp3-tree-node`
  }

  const getCaretClasses = () => {
    if (children) {
      return isExpanded
        ? 'bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard'
        : 'bp3-tree-node-caret bp3-tree-node-caret-close bp3-icon-standard'
    } else {
      return 'bp3-tree-node-caret-none bp3-icon-standard'
    }
  }

  const renderChildren = () => {
    const childNodes = resourceTree.get(name).get('children'),
      elementTreeNodes = childNodes
        .map((node, i) => {
          const path = getReferencePath(node)
          if (
            node.get('type') === 'Reference' &&
            node.get('referenceTypes').size <= 1
          ) {
            const referenceType = node.get('referenceTypes').first(),
              referencePath = `${path}.resolve()`
            return (
              <ResourceTreeNode
                name={node.get('name')}
                key={path}
                type={ResourceTreeNodeType.REFERENCE}
                referencePath={referencePath}
              />
            )
          } else {
            return (
              <ElementTreeNode
                {...node.delete('children').toJS()}
                key={path}
                path={path}
                treePath={[name, 'children', i]}
                resourceOrComplexType={name}
              />
            )
          }
        })
        .concat(renderReverseReferences())
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    )
  }

  const renderReverseReferences = () => {
    const nodes = reverseReferences
      .get(name)
      .filter(node => !!resourceTree.get(node.get('path').split('.')[0]))
    return nodes.map((node, i) => {
      const path = node.get('path'),
        referencePath = `${name}.reverseResolve(${path})`
      return (
        <ResourceTreeNode
          key={referencePath}
          name={path.split('.')[0]}
          label={path}
          type={ResourceTreeNodeType.REVERSE_REFERENCE}
          definition={node.get('definition')}
          referencePath={referencePath}
        />
      )
    })
  }

  const renderActionIcon = () =>
    referencePath ? null : (
      <span
        className="bp3-tree-node-secondary-label bp3-icon-standard bp3-icon-arrow-right"
        onClick={openContextMenu}
      />
    )

  const renderNodeContent = () => (
    <div className="inner">
      <div className="bp3-tree-node-content">
        <span
          className={getCaretClasses()}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className={getIconClasses()} />
        <span className="name bp3-tree-node-label" onClick={openContextMenu}>
          {label}
        </span>
        {renderActionIcon()}
      </div>
      {isExpanded && children ? renderChildren() : null}
    </div>
  )

  const renderNodeContentWithTooltip = () => {
    const definition = getDefinition(),
      tooltipContent = definition
        ? getDefinition()
        : `${name} data is not available on this server.`
    return (
      <Popover
        content={<div className="definition">{tooltipContent}</div>}
        position={Position.RIGHT}
        boundary={document.body}
        interactionKind={PopoverInteractionKind.HOVER}
        popoverClassName="bp3-dark"
        hoverOpenDelay={300}
      >
        {renderNodeContent()}
      </Popover>
    )
  }

  return (
    <li className={getNodeClasses()}>
      {isExpanded ? renderNodeContent() : renderNodeContentWithTooltip()}
    </li>
  )
}

export default connect(
  null,
  actions,
)(ResourceTreeNode)
