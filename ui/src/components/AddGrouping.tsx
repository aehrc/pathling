/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addGrouping } from "../store/QueryActions";

interface Props {
  path: string;
}

function AddGrouping(props: Props) {
  const { path } = props,
    expression = path;

  return (
    <MenuItem
      icon="graph"
      text={`Add "${expression}" to groupings`}
      onClick={() => store.dispatch(addGrouping(expression))}
    />
  );
}

export default AddGrouping;
