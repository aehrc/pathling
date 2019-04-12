import React from 'react'
import { Tree } from 'antd'
const { TreeNode } = Tree

function ElementTree(props) {
  const { resourceTree } = props

  return (
    <div>
      <Tree>{renderResourceTree(resourceTree)}</Tree>
    </div>
  )
}

function renderResourceTree(tree) {
  return Object.keys(tree).map(resourceName => {
    return (
      <TreeNode key={resourceName} title={resourceName}>
        {renderElements(tree[resourceName])}
      </TreeNode>
    )
  })
}

function renderElements(elements) {
  if (!elements) return null
  return elements.map(element => {
    return (
      <TreeNode key={element.name} title={element.name}>
        {renderElements(element.children)}
      </TreeNode>
    )
  })
}

export default ElementTree
