/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import { getResource, resourceTree } from "../fhir/ResourceTree";
import Resource from "./Resource";
import "./style/ElementTree.scss";

/**
 * Renders a tree showing resources and elements available for use within
 * analytic queries.
 *
 * @author John Grimes
 */
function ElementTree() {
  const resourceNodes = Object.keys(resourceTree).map((resourceName, i) => (
    <Resource {...getResource(resourceName)} key={i} name={resourceName} />
  ));

  return (
    <div className="element-tree">
      <ol className="resources">{resourceNodes}</ol>
    </div>
  );
}

export default ElementTree;
