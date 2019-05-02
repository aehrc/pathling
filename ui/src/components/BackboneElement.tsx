/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import { ElementNode } from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import "./style/BackboneElement.scss";
import TreeNodeTooltip from "./TreeNodeTooltip";

interface Props extends ElementNode {}

function BackboneElement(props: Props) {
  const { name, type, definition, contains } = props;

  const openContextMenu = () => {};

  return (
    <li className="backbone-element">
      <TreeNodeTooltip type={type} definition={definition}>
        <span className="caret" />
        <span className="icon" />
        <span className="label">{name}</span>
      </TreeNodeTooltip>
      <ol className="contains">{/*<ContainedElements nodes={contains} />*/}</ol>
    </li>
  );
}

export default BackboneElement;
