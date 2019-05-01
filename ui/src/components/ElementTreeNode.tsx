/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { ReactElement, useState } from "react";
import { connect } from "react-redux";
import {
  ContextMenu,
  Menu,
  MenuItem,
  Popover,
  PopoverInteractionKind,
  Position
} from "@blueprintjs/core";
import get from "lodash.get";

import * as actions from "../store/QueryActions";
import {
  complexTypesTree,
  ElementNode,
  resourceTree,
  supportedComplexTypes
} from "../fhir/ResourceTree";
import ResourceTreeNode from "./ResourceTreeNode";
import "./ElementTreeNode.scss";

const ConnectedElementTreeNode = connect(
  null,
  actions
)(ElementTreeNode);

interface Props {
  name: string;
  path: string;
  treePath: (string | number)[];
  type: string;
  definition: string;
  resourceOrComplexType: string;
  referenceTypes?: string[];
  addAggregation?: (expression: string) => void;
  addGrouping?: (expression: string) => void;
}

/**
 * Renders an individual element within the element tree, which may be a
 * primitive, BackboneElement or complex element.
 *
 * @author John Grimes
 */
function ElementTreeNode(props: Props) {
  const {
      name,
      path,
      treePath,
      type,
      definition,
      resourceOrComplexType,
      referenceTypes
    } = props,
    [isExpanded, setExpanded] = useState(false),
    isInResourceTree = resourceOrComplexType in resourceTree,
    backboneElementChildren = isInResourceTree
      ? get(resourceTree, treePath).children
      : null,
    isComplexType = supportedComplexTypes.includes(type),
    complexElementChildren = isComplexType
      ? complexTypesTree[type].children
      : null,
    isReference = type === "Reference",
    referenceChildren = isReference
      ? referenceTypes.filter(t => t in resourceTree)
      : null;

  /**
   * Opens a context menu at the supplied mouse event which provides actions for
   * adding the specified node to the current query.
   */
  const openContextMenu = (event: any): void => {
    const { addAggregation, addGrouping } = props,
      aggregationExpression = `${path}.count()`,
      groupingExpression = path,
      aggregationMenuItem = (
        <MenuItem
          icon="trending-up"
          text={`Add "${aggregationExpression}" to aggregations`}
          onClick={() => addAggregation(aggregationExpression)}
        />
      ),
      groupingMenuItem = (
        <MenuItem
          icon="graph"
          text={`Add "${groupingExpression}" to groupings`}
          onClick={() => addGrouping(groupingExpression)}
        />
      );
    ContextMenu.show(
      <Menu>
        {aggregationMenuItem}
        {groupingMenuItem}
      </Menu>,
      { left: event.clientX, top: event.clientY }
    );
  };

  const renderBackboneElementChildren = (): ReactElement => {
    const elementTreeNodes = backboneElementChildren.map(
      (node: ElementNode, i: number) => {
        const childPath = `${path}.${node.name}`,
          { children, ...nodeWithoutChildren } = node;
        return (
          <ConnectedElementTreeNode
            {...nodeWithoutChildren}
            key={childPath}
            path={childPath}
            treePath={treePath.concat("children", i)}
            resourceOrComplexType={resourceOrComplexType}
          />
        );
      }
    );
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    );
  };

  const renderComplexElementChildren = () => {
    const elementTreeNodes = complexElementChildren.map((node, i) => {
      const { children, ...nodeWithoutChildren } = node;
      return (
        <ConnectedElementTreeNode
          {...nodeWithoutChildren}
          key={path}
          path={`${path}.${node.name}`}
          treePath={[type, i]}
          resourceOrComplexType={isComplexType ? type : resourceOrComplexType}
        />
      );
    });
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    );
  };

  const renderReferenceChildren = () => {
    const resourceTreeNodes = referenceChildren.map(type => {
      const referencePath =
        referenceTypes.length > 1
          ? `${path}.resolve(${type})`
          : `${path}.resolve()`;
      return (
        <ResourceTreeNode
          name={type}
          key={referencePath}
          referencePath={referencePath}
        />
      );
    });
    return (
      <ol className="child-nodes bp3-tree-node-list">{resourceTreeNodes}</ol>
    );
  };

  const renderActionIcon = () =>
    isComplexType || isReference ? null : (
      <span
        className="bp3-tree-node-secondary-label bp3-icon-standard bp3-icon-arrow-right"
        onClick={openContextMenu}
      />
    );

  const renderNodeContent = () => (
    <div className="inner">
      <div className="bp3-tree-node-content">
        <span
          className={getCaretClasses()}
          onClick={() => setExpanded(!isExpanded)}
        />
        <span className={getIconClasses()} />
        <span className="name bp3-tree-node-label">{name}</span>
        {renderActionIcon()}
      </div>
      {isExpanded && backboneElementChildren
        ? renderBackboneElementChildren()
        : null}
      {isExpanded && complexElementChildren
        ? renderComplexElementChildren()
        : null}
      {isExpanded && referenceChildren ? renderReferenceChildren() : null}
    </div>
  );

  const getCaretClasses = () => {
    if (
      backboneElementChildren ||
      complexElementChildren ||
      (referenceChildren && referenceChildren.length > 0)
    ) {
      return isExpanded
        ? "bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"
        : "bp3-tree-node-caret bp3-tree-node-caret-close bp3-icon-standard";
    } else {
      return "bp3-tree-node-caret-none bp3-icon-standard";
    }
  };

  const getIconClasses = () => {
    let iconName = null;
    if (isComplexType) {
      iconName = "grid-view";
    } else if (isReference) {
      iconName = "document-share";
    } else if (backboneElementChildren) {
      iconName = "folder-close";
    } else {
      iconName = "symbol-square";
    }
    return `bp3-tree-node-icon bp3-icon-standard bp3-icon-${iconName}`;
  };

  return (
    <li className="element-tree-node bp3-tree-node">
      {isExpanded ? (
        renderNodeContent()
      ) : (
        <Popover
          content={<div className="definition">{definition}</div>}
          position={Position.RIGHT}
          boundary="viewport"
          interactionKind={PopoverInteractionKind.HOVER}
          popoverClassName="bp3-dark"
          hoverOpenDelay={300}
        >
          {renderNodeContent()}
        </Popover>
      )}
    </li>
  );
}

export default ConnectedElementTreeNode;
