/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { QueryAction } from "./QueryActions";

export interface Query {
  aggregations: Aggregation[];
  groupings: Grouping[];
  filters: Filter[];
}

export interface QueryState {
  id?: string;
  name?: string;
  unsavedChanges: boolean;
  query: Query;
}

export interface Aggregation {
  label?: string;
  expression: string;
}

export interface PartialAggregation {
  label?: string;
  expression?: string;
}

export interface Grouping {
  label?: string;
  expression: string;
}

export interface PartialGrouping {
  label?: string;
  expression?: string;
}

export interface Filter {
  label?: string;
  expression: string;
}

export interface PartialFilter {
  label?: string;
  expression?: string;
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
          aggregations: state.query.aggregations.concat({
            expression: action.expression,
            label: action.expression
          })
        },
        unsavedChanges: true
      };
    case "REMOVE_AGGREGATION":
      return {
        ...state,
        query: {
          ...state.query,
          aggregations: state.query.aggregations.filter(
            (_, i) => i !== action.index
          )
        },
        unsavedChanges: true
      };
    case "UPDATE_AGGREGATION":
      return {
        ...state,
        query: {
          ...state.query,
          aggregations: state.query.aggregations.map((aggregation, i) =>
            i === action.index
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
          groupings: state.query.groupings.concat({
            expression: action.expression,
            label: action.expression
          })
        },
        unsavedChanges: true
      };
    case "REMOVE_GROUPING":
      return {
        ...state,
        query: {
          ...state.query,
          groupings: state.query.groupings.filter((_, i) => i !== action.index)
        },
        unsavedChanges: true
      };
    case "UPDATE_GROUPING":
      return {
        ...state,
        query: {
          ...state.query,
          groupings: state.query.groupings.map((grouping, i) =>
            i === action.index ? { ...grouping, ...action.grouping } : grouping
          )
        },
        unsavedChanges: true
      };
    case "ADD_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.concat({
            expression: action.expression,
            label: action.expression
          })
        },
        unsavedChanges: true
      };
    case "REMOVE_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.filter((_, i) => i !== action.index)
        },
        unsavedChanges: true
      };
    case "UPDATE_FILTER":
      return {
        ...state,
        query: {
          ...state.query,
          filters: state.query.filters.map((filter, i) =>
            i === action.index ? { ...filter, ...action.filter } : filter
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
