/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import {
  PartialAggregation,
  PartialFilter,
  PartialGrouping
} from "./QueryReducer";
import { SavedQuery } from "./SavedQueriesReducer";

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

export interface AddFilter {
  type: "ADD_FILTER";
  expression: string;
}

export interface RemoveFilter {
  type: "REMOVE_FILTER";
  index: number;
}

export interface UpdateFilter {
  type: "UPDATE_FILTER";
  index: number;
  filter: PartialFilter;
}

export interface ClearQuery {
  type: "CLEAR_QUERY";
}

export interface LoadQuery {
  type: "LOAD_QUERY";
  name: string;
  query: SavedQuery;
}

export type QueryAction =
  | AddAggregation
  | RemoveAggregation
  | UpdateAggregation
  | AddGrouping
  | RemoveGrouping
  | UpdateGrouping
  | AddFilter
  | RemoveFilter
  | UpdateFilter
  | ClearQuery
  | LoadQuery;

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

export const addFilter = (expression: string): AddFilter => ({
  type: "ADD_FILTER",
  expression
});

export const removeFilter = (index: number): RemoveFilter => ({
  type: "REMOVE_FILTER",
  index
});

export const updateFilter = (index: number, filter: PartialFilter) => ({
  type: "UPDATE_FILTER",
  index,
  filter
});

export const clearQuery = (): ClearQuery => ({
  type: "CLEAR_QUERY"
});

export const loadQuery = (query: SavedQuery) => ({
  type: "LOAD_QUERY",
  name,
  query
});
