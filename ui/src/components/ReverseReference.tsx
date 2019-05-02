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

interface Props extends ElementNode {}

function ReverseReference(props: Props) {
  const { path, referenceTypes } = props,
    unsupported =
      referenceTypes.length === 1 && !(referenceTypes[0] in resourceTree);

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
    <UnsupportedReference {...props} />
  ) : (
    <li className="reverse-reference">
      <div className="inner">
        <div className="content">
          <span className="caret" />
          <span className="icon" />
          <span className="label">{path}</span>
        </div>
        {/*<ol className="contains">{renderContains()}</ol>*/}
      </div>
    </li>
  );
}

export default ReverseReference;
