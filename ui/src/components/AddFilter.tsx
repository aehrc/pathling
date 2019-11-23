/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { MenuItem } from "@blueprintjs/core";
import * as React from "react";

import store from "../store";
import { addFilter, focusExpression } from "../store/QueryActions";

interface Props {
  path: string;
}

function AddFilter(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const addFilterAction = addFilter({ label: expression, expression });
    store.dispatch(addFilterAction);
    store.dispatch(focusExpression(addFilterAction.filter.id));
  };

  const handleTabIndexedKeyDown = (event: any) => {
    if (event.key === "Enter") {
      event.target.click();
    }
  };

  return (
    <MenuItem
      icon="filter"
      text={`Add "${expression}" to filters`}
      onClick={handleClick}
      onKeyDown={handleTabIndexedKeyDown}
      tabIndex={0}
    />
  );
}

export default AddFilter;
