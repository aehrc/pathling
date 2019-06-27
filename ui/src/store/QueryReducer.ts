/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryAction } from "./QueryActions";

export interface Query {
  aggregations: ExpressionWithIdentity[];
  groupings: ExpressionWithIdentity[];
  filters: ExpressionWithIdentity[];
}

export interface QueryState {
  id?: string;
  name?: string;
  unsavedChanges: boolean;
  query: Query;
}

export interface Expression {
  label?: string;
  expression?: string;
}

export interface ExpressionWithIdentity extends Expression {
  id: string;
}

const initialState: QueryState = {
  query: {
    aggregations: [],
    groupings: [],
    filters: []
  },
  unsavedChanges: false
};

export default (state = initialState, action: QueryAction): QueryState => {
  switch (action.type) {
    case "ADD_AGGREGATION":
      return {
        ...state,
        query: {
          ...state.query,
          aggregations: state.query.aggregations.concat(action.aggregation)
        },
        unsavedChanges: true
      };
    case "REMOVE_AGGREGATION":
      return {
        ...state,
        query: {
          ...state.query,
          aggregations: state.query.aggregations.filter(
            aggregation => aggregation.id !== action.id
          )
        },
        unsavedChanges: true
      };
    case "UPDATE_AGGREGATION":
      return {
        ...state,
        query: {
          ...state.query,
          aggregations: state.query.aggregations.map(aggregation =>
            aggregation.id === action.aggregation.id
              ? { ...aggregation, ...action.aggregation }
              : aggregation
          )
        },
        unsavedChanges: true
      };
    case "ADD_GROUPING":
      return {
        ...state,
        query: {
          ...state.query,
          groupings: state.query.groupings.concat(action.grouping)
        },
        unsavedChanges: true
      };
    case "REMOVE_GROUPING":
      return {
        ...state,
        query: {
          ...state.query,
          groupings: state.query.groupings.filter(
            grouping => grouping.id !== action.id
          )
        },
        unsavedChanges: true
      };
    case "UPDATE_GROUPING":
      return {
        ...state,
        query: {
          ...state.query,
          groupings: state.query.groupings.map(grouping =>
            grouping.id === action.grouping.id
              ? { ...grouping, ...action.grouping }
              : grouping
          )
        },
        unsavedChanges: true
      };
    case "ADD_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.concat(action.filter)
        },
        unsavedChanges: true
      };
    case "REMOVE_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.filter(filter => filter.id !== action.id)
        },
        unsavedChanges: true
      };
    case "UPDATE_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.map(filter =>
            filter.id === action.filter.id
              ? { ...filter, ...action.filter }
              : filter
          )
        },
        unsavedChanges: true
      };
    case "CLEAR_QUERY":
      return initialState;
    case "LOAD_QUERY":
      return {
        ...state,
        ...action.query,
        unsavedChanges: false
      };
    default:
      return state;
  }
};
