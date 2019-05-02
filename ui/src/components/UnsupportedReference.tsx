/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import { ElementNode } from "../fhir/ResourceTree";
import "./style/UnsupportedReference.scss";

interface Props extends ElementNode {}

function UnsupportedReference(props: Props) {
  const { name } = props;

  const openContextMenu = () => {};

  return (
    <li className="unsupported-reference">
      <div className="inner">
        <div className="content">
          <span className="caret-none" />
          <span className="icon" />
          <span className="label">{name}</span>
        </div>
      </div>
    </li>
  );
}

export default UnsupportedReference;
