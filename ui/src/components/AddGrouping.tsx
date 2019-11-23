/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { MenuItem } from "@blueprintjs/core";
import * as React from "react";

import store from "../store";
import { addGrouping, focusExpression } from "../store/QueryActions";

interface Props {
  path: string;
}

function AddGrouping(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const addGroupingAction = addGrouping({ label: expression, expression });
    store.dispatch(addGroupingAction);
    store.dispatch(focusExpression(addGroupingAction.grouping.id));
  };

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  return (
    <MenuItem
      icon="graph"
      text={`Add "${expression}" to groupings`}
      onClick={handleClick}
      onKeyDown={handleTabIndexedKeyDown}
      tabIndex={0}
    />
  );
}

export default AddGrouping;
