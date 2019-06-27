/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";
import { addAggregation } from "../store/QueryActions";
import { setElementTreeFocus } from "../store/ElementTreeActions";

interface Props {
  path: string;
}

function AddAggregation(props: Props) {
  const { path } = props,
    expression = `${path}.count()`;

  const handleClick = () => {
    const focus = store.getState().elementTree.focus;
    store.dispatch(addAggregation({ expression }));
    if (focus === null)
      store.dispatch(
        setElementTreeFocus(getSubjectResourceFromExpression(path))
      );
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
