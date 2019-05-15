/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addFilter } from "../store/QueryActions";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";

interface Props {
  path: string;
}

function AddFilter(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const focus = store.getState().elementTree.focus;
    store.dispatch(addFilter(expression));
    if (focus === null)
      store.dispatch(
        setElementTreeFocus(getSubjectResourceFromExpression(path))
      );
  };

  return (
    <MenuItem
      icon="filter"
      text={`Add "${expression}" to filters`}
      onClick={handleClick}
    />
  );
}

export default AddFilter;
