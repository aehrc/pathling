/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import uuidv4 from "uuid/v4";
import { Expression, ExpressionWithIdentity } from "./QueryReducer";
import { SavedQuery } from "./SavedQueriesReducer";

export interface SetSubjectResource {
  type: "SET_SUBJECT_RESOURCE";
  subjectResource: string;
}

export interface AddAggregation {
  type: "ADD_AGGREGATION";
  aggregation: ExpressionWithIdentity;
}

export interface RemoveAggregation {
  type: "REMOVE_AGGREGATION";
  id: string;
}

export interface UpdateAggregation {
  type: "UPDATE_AGGREGATION";
  aggregation: ExpressionWithIdentity;
}

export interface AddGrouping {
  type: "ADD_GROUPING";
  grouping: ExpressionWithIdentity;
}

export interface RemoveGrouping {
  type: "REMOVE_GROUPING";
  id: string;
}

export interface UpdateGrouping {
  type: "UPDATE_GROUPING";
  grouping: ExpressionWithIdentity;
}

export interface AddFilter {
  type: "ADD_FILTER";
  filter: ExpressionWithIdentity;
}

export interface RemoveFilter {
  type: "REMOVE_FILTER";
  id: string;
}

export interface UpdateFilter {
  type: "UPDATE_FILTER";
  filter: ExpressionWithIdentity;
}

export interface ClearQuery {
  type: "CLEAR_QUERY";
}

export interface LoadQuery {
  type: "LOAD_QUERY";
  name: string;
  query: SavedQuery;
}

export interface FocusExpression {
  type: "FOCUS_EXPRESSION";
  id: string;
}

export interface ReceiveExpressionFocus {
  type: "RECEIVE_EXPRESSION_FOCUS";
}

export type QueryAction =
  | SetSubjectResource
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
  | LoadQuery
  | FocusExpression
  | ReceiveExpressionFocus;

export const setSubjectResource = (subjectResource: string): SetSubjectResource => ({
  type: "SET_SUBJECT_RESOURCE",
  subjectResource
});

export const addAggregation = (aggregation: Expression): AddAggregation => ({
  type: "ADD_AGGREGATION",
  aggregation: { ...aggregation, id: uuidv4() }
});

export const removeAggregation = (id: string): RemoveAggregation => ({
  type: "REMOVE_AGGREGATION",
  id
});

export const updateAggregation = (aggregation: ExpressionWithIdentity) => ({
  type: "UPDATE_AGGREGATION",
  aggregation
});

export const addGrouping = (grouping: Expression): AddGrouping => ({
  type: "ADD_GROUPING",
  grouping: { ...grouping, id: uuidv4() }
});

export const removeGrouping = (id: string): RemoveGrouping => ({
  type: "REMOVE_GROUPING",
  id
});

export const updateGrouping = (grouping: ExpressionWithIdentity) => ({
  type: "UPDATE_GROUPING",
  grouping
});

export const addFilter = (filter: Expression): AddFilter => ({
  type: "ADD_FILTER",
  filter: { ...filter, id: uuidv4() }
});

export const removeFilter = (id: string): RemoveFilter => ({
  type: "REMOVE_FILTER",
  id
});

export const updateFilter = (filter: ExpressionWithIdentity) => ({
  type: "UPDATE_FILTER",
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

export const focusExpression = (id: string) => ({
  type: "FOCUS_EXPRESSION",
  id
});

export const receiveExpressionFocus = () => ({
  type: "RECEIVE_EXPRESSION_FOCUS"
});
