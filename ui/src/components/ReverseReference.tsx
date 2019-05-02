/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";

import {
  ElementNode,
  getResource,
  getReverseReferences,
  resourceTree
} from "../fhir/ResourceTree";
import Resource from "./Resource";
import ContainedElements from "./ContainedElements";
import UnsupportedReference from "./UnsupportedReference";
import "./style/ReverseReference.scss";
import TreeNodeTooltip from "./TreeNodeTooltip";

interface Props extends ElementNode {}

function ReverseReference(props: Props) {
  const { path, type, definition, referenceTypes } = props,
    pathComponents = path.split("."),
    sourceType = pathComponents[0],
    unsupported = !(sourceType in resourceTree);

  const openContextMenu = () => {};

  // const renderContains = () =>
  //   referenceTypes.length > 1 ? renderResources() : renderContainsDirectly();
  //
  // const renderResources = () =>
  //   referenceTypes
  //     .filter(referenceType => referenceType in resourceTree)
  //     .map(referenceType => {
  //       const resourceNode = getResource(referenceType);
  //       return <Resource {...resourceNode} name={referenceType} />;
  //     });
  //
  // const renderContainsDirectly = () => {
  //   const referenceType = referenceTypes[0],
  //     contains = getResource(referenceType).contains,
  //     reverseReferenceNodes = getReverseReferences(referenceType).map(node => (
  //       <ReverseReference {...node} />
  //     ));
  //   return [<ContainedElements nodes={contains} />].concat(
  //     reverseReferenceNodes
  //   );
  // };

  return unsupported ? (
    <UnsupportedReference {...props} reverse />
  ) : (
    <li className="reverse-reference">
      <TreeNodeTooltip
        type={type}
        definition={definition}
        referenceTypes={referenceTypes}
      >
        <span className="caret" />
        <span className="icon" />
        <span className="label">{path}</span>
      </TreeNodeTooltip>
      {/*<ol className="contains">{renderContains()}</ol>*/}
    </li>
  );
}

export default ReverseReference;
