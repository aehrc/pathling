/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

import { Map, fromJS } from 'immutable'

import rawResourceTree from '../../../config/resource-tree.json'
import rawComplexTypesTree from '../../../config/complex-type-tree.json'

export const resourceTree = Map(fromJS(rawResourceTree))
export const complexTypesTree = Map(fromJS(rawComplexTypesTree))

// Reference is supported, but is left out of this list as it is treated
// differently.
export const supportedComplexTypes = [
  'Ratio',
  'Period',
  'Range',
  'Attachment',
  'Identifier',
  'HumanName',
  'Annotation',
  'Address',
  'ContactPoint',
  'SampledData',
  'Money',
  'Count',
  'Duration',
  'SimpleQuantity',
  'Quantity',
  'Distance',
  'Age',
  'CodeableConcept',
  'Signature',
  'Coding',
  'Timing',
]
