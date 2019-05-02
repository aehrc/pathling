/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import { ElementNode } from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import "./style/BackboneElement.scss";

interface Props extends ElementNode {}

function BackboneElement(props: Props) {
  const { name, contains } = props;

  const openContextMenu = () => {};

  return (
    <li className="backbone-element">
      <div className="inner">
        <div className="content">
          <span className="caret" />
          <span className="icon" />
          <span className="label">{name}</span>
        </div>
        <ol className="contains">
          {/*<ContainedElements nodes={contains} />*/}
        </ol>
      </div>
    </li>
  );
}

export default BackboneElement;
