/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addGrouping } from "../store/QueryActions";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";

interface Props {
  path: string;
}

function AddGrouping(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const focus = store.getState().elementTree.focus;
    store.dispatch(addGrouping(expression));
    if (focus === null)
      store.dispatch(
        setElementTreeFocus(getSubjectResourceFromExpression(path))
      );
  };

  return (
    <MenuItem
      icon="graph"
      text={`Add "${expression}" to groupings`}
      onClick={handleClick}
    />
  );
}

export default AddGrouping;
