/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

export interface SetElementTreeFocus {
  type: "SET_ELEMENT_TREE_FOCUS";
  focus: string;
}

export interface ClearElementTreeFocus {
  type: "CLEAR_ELEMENT_TREE_FOCUS";
}

export type ElementTreeAction = SetElementTreeFocus | ClearElementTreeFocus;

export const setElementTreeFocus = (focus: string): SetElementTreeFocus => ({
  type: "SET_ELEMENT_TREE_FOCUS",
  focus
});

export const clearElementTreeFocus = (): ClearElementTreeFocus => ({
  type: "CLEAR_ELEMENT_TREE_FOCUS"
});
