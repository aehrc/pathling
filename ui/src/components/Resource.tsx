/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import React, { useState } from "react";
import { ContextMenu, Menu } from "@blueprintjs/core";

import {
  getReverseReferences,
  ResourceNode,
  reverseReferences
} from "../fhir/ResourceTree";
import ContainedElements from "./ContainedElements";
import ReverseReference from "./ReverseReference";
import AddAggregation from "./AddAggregation";
import "./style/Resource.scss";

interface Props extends ResourceNode {
  name: string;
}

function Resource(props: Props) {
  const { name, contains } = props,
    [isExpanded, setExpanded] = useState(false);

  const openContextMenu = (event: any): void => {
    ContextMenu.show(
      <Menu>
        <AddAggregation path={name} />
      </Menu>,
      {
        left: event.clientX,
        top: event.clientY
      }
    );
  };

  const renderContains = () => {
    const reverseReferenceNodes =
      name in reverseReferences
        ? getReverseReferences(name).map((node, i) => (
            <ReverseReference {...node} key={i + 1} />
          ))
        : [];
    return [
      <ContainedElements key={0} nodes={contains} parentPath={name} />
    ].concat(reverseReferenceNodes);
  };

  return (
    <li className="resource">
      <div className="inner">
        <div className="content">
          <span
            className={isExpanded ? "caret-open" : "caret-closed"}
            onClick={() => setExpanded(!isExpanded)}
          />
          <span className="icon" />
          <span className="label">{name}</span>
          <span className="action" onClick={openContextMenu} />
        </div>
        {isExpanded ? <ol className="contains">{renderContains()}</ol> : null}
      </div>
    </li>
  );
}

export default Resource;
