/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

const fs = require('fs')

process.stdin.setEncoding('utf8')

let data = ''

process.stdin.on('readable', () => {
  const chunk = process.stdin.read()
  if (chunk !== null) {
    data += chunk
  }
})

process.stdin.on('end', () => {
  const resourceDefs = JSON.parse(data),
    complexTypePath = process.argv[2],
    complexTypeData = fs.readFileSync(complexTypePath, { encoding: 'utf8' }),
    complexTypeDefs = JSON.parse(complexTypeData),
    reverseReferences = transformElementMapToReverseReferences(
      resourceDefs,
      complexTypeDefs,
    )
  process.stdout.write(JSON.stringify(reverseReferences, null, 2))
})

function transformElementMapToReverseReferences(resourceDefs, complexTypeDefs) {
  let reverseReferences = {}
  for (const resourceName of Object.keys(resourceDefs)) {
    if (resourceName) if (!(resourceName in resourceDefs)) continue
    reverseReferences = getReverseReferences({
      path: resourceName,
      children: resourceDefs[resourceName]['children'],
      reverseReferences,
      complexTypeDefs,
    })
  }
  return reverseReferences
}

function getReverseReferences(options) {
  const { path, children, complexTypeDefs } = options
  let { reverseReferences } = options
  for (const element of children) {
    const elementType = element['type']
    if (elementType === 'Reference') {
      for (const referenceType of element['referenceTypes']) {
        reverseReferences[referenceType] =
          referenceType in reverseReferences
            ? reverseReferences[referenceType]
            : []
        reverseReferences[referenceType] = [
          ...reverseReferences[referenceType],
          element,
          // `${path}.${element['name']}`,
        ]
      }
    } else if (Object.keys(complexTypeDefs).includes(elementType)) {
      reverseReferences = getReverseReferences({
        ...options,
        path: `${path}.${element['name']}`,
        children: complexTypeDefs[elementType]['children'],
      })
    } else if (element['children']) {
      reverseReferences = getReverseReferences({
        ...options,
        path: `${path}.${element['name']}`,
        children: element['children'],
      })
    }
  }
  return reverseReferences
}
