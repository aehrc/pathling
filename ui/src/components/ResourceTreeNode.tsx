/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { ReactElement, useState } from "react";
import { connect } from "react-redux";
import {
  ContextMenu,
  Menu,
  MenuItem,
  Popover,
  PopoverInteractionKind,
  Position
} from "@blueprintjs/core";

import ElementTreeNode from "./ElementTreeNode";
import {
  ElementNode,
  resourceTree,
  reverseReferences
} from "../fhir/ResourceTree";
import * as actions from "../store";
import "./ResourceTreeNode.scss";

interface Props {
  name: string;
  type?: ResourceTreeNodeType;
  label?: string;
  definition?: string;
  resourceType?: string;
  referencePath?: string;
  addAggregation?: (expression: string) => void;
}

enum ResourceTreeNodeType {
  Resource = "RESOURCE",
  Reference = "REFERENCE",
  ReverseReference = "REVERSE_REFERENCE"
}

/**
 * Renders a tree node showing the elements available within a particular
 * resource type.
 *
 * @author John Grimes
 */
function ResourceTreeNode(props: Props) {
  const { name, type, resourceType, referencePath, addAggregation } = props,
    label = props.label ? props.label : name,
    children = name in resourceTree,
    [isExpanded, setExpanded] = useState(false);

  const openContextMenu = (event: any): void => {
    const aggregationExpression = `${name}.count()`,
      aggregationLabel = aggregationExpression,
      aggregationMenuItem = (
        <MenuItem
          icon="trending-up"
          text={`Add "${aggregationExpression}" to aggregations`}
          onClick={() => addAggregation(aggregationExpression)}
        />
      );

    ContextMenu.show(<Menu>{aggregationMenuItem}</Menu>, {
      left: event.clientX,
      top: event.clientY
    });
  };

  const getReferencePath = (node: ElementNode): string => {
    if (!referencePath) return node.path;
    const pathSuffix = node.path
      .split(".")
      .slice(1)
      .join(".");
    return `${referencePath}.${pathSuffix}`;
  };

  const getDefinition = (): string | null => {
    if (props.definition) {
      return props.definition;
    } else {
      return children ? resourceTree[name].definition : null;
    }
  };

  const getIconClasses = (): string => {
    let iconName = null;
    if (type === "REFERENCE") {
      iconName = "document-share";
    } else if (type === "REVERSE_REFERENCE") {
      iconName = "document-open";
    } else {
      iconName = "cube";
    }
    return `bp3-tree-node-icon bp3-icon-standard bp3-icon-${iconName}`;
  };

  const getNodeClasses = (): string => {
    let modifier = "";
    if (type === "REFERENCE") {
      modifier = "resource-tree-node-reference";
    } else if (type === "REVERSE_REFERENCE") {
      modifier = "resource-tree-node-reverse-reference";
    }
    return isExpanded
      ? `resource-tree-node ${modifier}bp3-tree-node bp3-tree-node-expanded`
      : `resource-tree-node ${modifier}bp3-tree-node`;
  };

  const getCaretClasses = (): string => {
    if (children) {
      return isExpanded
        ? "bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"
        : "bp3-tree-node-caret bp3-tree-node-caret-close bp3-icon-standard";
    } else {
      return "bp3-tree-node-caret-none bp3-icon-standard";
    }
  };

  const renderChildren = (): ReactElement => {
    const childNodes = resourceTree[name].children,
      elementTreeNodes = childNodes
        .map((node, i) => {
          const path = getReferencePath(node);
          if (node.type === "Reference" && node.referenceTypes.length <= 1) {
            const referenceType = node.referenceTypes[0],
              referencePath = `${path}.resolve()`;
            return (
              <ResourceTreeNode
                name={node.name}
                key={path}
                type={ResourceTreeNodeType.Reference}
                referencePath={referencePath}
              />
            );
          } else {
            const { children, ...nodeWithoutChildren } = node;
            return (
              <ElementTreeNode
                {...nodeWithoutChildren}
                key={path}
                path={path}
                treePath={[name, "children", i]}
                resourceOrComplexType={name}
              />
            );
          }
        })
        .concat(renderReverseReferences());
    return (
      <ol className="child-nodes bp3-tree-node-list">{elementTreeNodes}</ol>
    );
  };

  const renderReverseReferences = (): ReactElement[] => {
    const nodes = reverseReferences[name].filter(
      node => node.path.split(".")[0] in resourceTree
    );
    return nodes.map((node, i) => {
      const path = node.path,
        referencePath = `${name}.reverseResolve(${path})`;
      return (
        <ResourceTreeNode
          key={referencePath}
          name={path.split(".")[0]}
          label={path}
          type={ResourceTreeNodeType.ReverseReference}
          definition={node.definition}
          referencePath={referencePath}
        />
      );
    });
  };

  const renderActionIcon = () =>
    referencePath ? null : (
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
        <span className="name bp3-tree-node-label" onClick={openContextMenu}>
          {label}
        </span>
        {renderActionIcon()}
      </div>
      {isExpanded && children ? renderChildren() : null}
    </div>
  );

  const renderNodeContentWithTooltip = () => {
    const definition = getDefinition(),
      tooltipContent = definition
        ? getDefinition()
        : `${name} data is not available on this server.`;
    return (
      <Popover
        content={<div className="definition">{tooltipContent}</div>}
        position={Position.RIGHT}
        boundary="viewport"
        interactionKind={PopoverInteractionKind.HOVER}
        popoverClassName="bp3-dark"
        hoverOpenDelay={300}
      >
        {renderNodeContent()}
      </Popover>
    );
  };

  return (
    <li className={getNodeClasses()}>
      {isExpanded ? renderNodeContent() : renderNodeContentWithTooltip()}
    </li>
  );
}

export default connect(
  null,
  actions
)(ResourceTreeNode);
