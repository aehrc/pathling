/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { PartialAggregation, PartialGrouping } from "./QueryReducer";

export interface AddAggregation {
  type: "ADD_AGGREGATION";
  expression: string;
}

export interface RemoveAggregation {
  type: "REMOVE_AGGREGATION";
  index: number;
}

export interface UpdateAggregation {
  type: "UPDATE_AGGREGATION";
  index: number;
  aggregation: PartialAggregation;
}

export interface AddGrouping {
  type: "ADD_GROUPING";
  expression: string;
}

export interface RemoveGrouping {
  type: "REMOVE_GROUPING";
  index: number;
}

export interface UpdateGrouping {
  type: "UPDATE_GROUPING";
  index: number;
  grouping: PartialGrouping;
}

export interface ClearQuery {
  type: "CLEAR_QUERY";
}

export type QueryAction =
  | AddAggregation
  | RemoveAggregation
  | UpdateAggregation
  | AddGrouping
  | RemoveGrouping
  | UpdateGrouping
  | ClearQuery;

export const addAggregation = (expression: string): AddAggregation => ({
  type: "ADD_AGGREGATION",
  expression
});

export const removeAggregation = (index: number): RemoveAggregation => ({
  type: "REMOVE_AGGREGATION",
  index
});

export const updateAggregation = (
  index: number,
  aggregation: PartialAggregation
) => ({
  type: "UPDATE_AGGREGATION",
  index,
  aggregation
});

export const addGrouping = (expression: string): AddGrouping => ({
  type: "ADD_GROUPING",
  expression
});

export const removeGrouping = (index: number): RemoveGrouping => ({
  type: "REMOVE_GROUPING",
  index
});

export const updateGrouping = (index: number, grouping: PartialGrouping) => ({
  type: "UPDATE_GROUPING",
  index,
  grouping
});

export const clearQuery = (): ClearQuery => ({
  type: "CLEAR_QUERY"
});
