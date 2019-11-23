/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { MenuItem } from "@blueprintjs/core";
import * as React from "react";

import store from "../store";
import { addAggregation, focusExpression } from "../store/QueryActions";

interface Props {
  path: string;
}

function AddAggregation(props: Props) {
  const { path } = props,
    expression = `${path}.count()`;

  const handleClick = () => {
    const addAggregationAction = addAggregation({
      label: expression,
      expression
    });
    store.dispatch(addAggregationAction);
    store.dispatch(focusExpression(addAggregationAction.aggregation.id));
  };

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  return (
    <MenuItem
      icon="trending-up"
      text={`Add "${expression}" to aggregations`}
      onClick={handleClick}
      onKeyDown={handleTabIndexedKeyDown}
      tabIndex={0}
    />
  );
}

export default AddAggregation;
