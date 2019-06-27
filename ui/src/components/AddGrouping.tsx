/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addGrouping, focusExpression } from "../store/QueryActions";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";

interface Props {
  path: string;
}

function AddGrouping(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const focus = store.getState().elementTree.focus,
      addGroupingAction = addGrouping({ label: expression, expression });
    store.dispatch(addGroupingAction);
    if (focus === null)
      store.dispatch(
        setElementTreeFocus(getSubjectResourceFromExpression(path))
      );
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
