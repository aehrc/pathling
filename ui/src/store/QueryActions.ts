/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

export interface AddAggregation {
  type: "ADD_AGGREGATION";
  expression: string;
}

export interface RemoveAggregation {
  type: "REMOVE_AGGREGATION";
  index: number;
}

export interface AddGrouping {
  type: "ADD_GROUPING";
  expression: string;
}

export interface RemoveGrouping {
  type: "REMOVE_GROUPING";
  index: number;
}

export interface ClearQuery {
  type: "CLEAR_QUERY";
}

export type QueryAction =
  | AddAggregation
  | RemoveAggregation
  | AddGrouping
  | RemoveGrouping
  | ClearQuery;

export const addAggregation = (expression: string): AddAggregation => ({
  type: "ADD_AGGREGATION",
  expression
});

export const removeAggregation = (index: number): RemoveAggregation => ({
  type: "REMOVE_AGGREGATION",
  index
});

export const addGrouping = (expression: string): AddGrouping => ({
  type: "ADD_GROUPING",
  expression
});
export const removeGrouping = (index: number): RemoveGrouping => ({
  type: "REMOVE_GROUPING",
  index
});

export const clearQuery = (): ClearQuery => ({
  type: "CLEAR_QUERY"
});
