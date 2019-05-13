/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { ElementTreeAction } from "./ElementTreeActions";

export interface ElementTreeState {
  focus?: string;
}

const initialState: ElementTreeState = {
  focus: null
};

export default (
  state = initialState,
  action: ElementTreeAction
): ElementTreeState => {
  switch (action.type) {
    case "SET_ELEMENT_TREE_FOCUS":
      return {
        ...state,
        focus: action.focus
      };
    case "CLEAR_ELEMENT_TREE_FOCUS":
      return initialState;
    default:
      return state;
  }
};
