/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import * as React from "react";
import { MenuItem } from "@blueprintjs/core";

import store from "../store";
import { addFilter, focusExpression } from "../store/QueryActions";
import { setElementTreeFocus } from "../store/ElementTreeActions";
import { getSubjectResourceFromExpression } from "../fhir/ResourceTree";

interface Props {
  path: string;
}

function AddFilter(props: Props) {
  const { path } = props,
    expression = path;

  const handleClick = () => {
    const focus = store.getState().elementTree.focus,
      addFilterAction = addFilter({ label: expression, expression });
    store.dispatch(addFilterAction);
    if (focus === null)
      store.dispatch(
        setElementTreeFocus(getSubjectResourceFromExpression(path))
      );
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
