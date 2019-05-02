/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addAggregation } from "../store/QueryActions";

interface Props {
  path: string;
}

function AddAggregation(props: Props) {
  const { path } = props,
    expression = `${path}.count()`;

  return (
    <MenuItem
      icon="trending-up"
      text={`Add "${expression}" to aggregations`}
      onClick={() => store.dispatch(addAggregation(expression))}
    />
  );
}

export default AddAggregation;
