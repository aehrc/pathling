/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

const fs = require("fs");

process.stdin.setEncoding("utf8");

let data = "";

process.stdin.on("readable", () => {
  const chunk = process.stdin.read();
  if (chunk !== null) {
    data += chunk;
  }
});

process.stdin.on("end", () => {
  const resourceDefs = JSON.parse(data),
    complexTypePath = process.argv[2],
    complexTypeData = fs.readFileSync(complexTypePath, { encoding: "utf8" }),
    complexTypeDefs = JSON.parse(complexTypeData),
    reverseReferences = transformElementMapToReverseReferences(
      resourceDefs,
      complexTypeDefs
    );
  process.stdout.write(JSON.stringify(reverseReferences, null, 2));
});

function transformElementMapToReverseReferences(resourceDefs, complexTypeDefs) {
  let reverseReferences = {};
  for (const resourceName of Object.keys(resourceDefs)) {
    if (resourceName) if (!(resourceName in resourceDefs)) continue;
    reverseReferences = getReverseReferences({
      path: resourceName,
      contains: resourceDefs[resourceName]["contains"],
      reverseReferences,
      complexTypeDefs
    });
  }
  return reverseReferences;
}

function getReverseReferences(options) {
  const { path, contains, complexTypeDefs } = options;
  let { reverseReferences } = options;
  for (const element of contains) {
    const elementType = element["type"];
    if (elementType === "Reference") {
      for (const referenceType of element["referenceTypes"]) {
        reverseReferences[referenceType] =
          referenceType in reverseReferences
            ? reverseReferences[referenceType]
            : [];
        reverseReferences[referenceType] = [
          ...reverseReferences[referenceType],
          element
          // `${path}.${element['name']}`,
        ];
      }
    } else if (Object.keys(complexTypeDefs).includes(elementType)) {
      reverseReferences = getReverseReferences({
        ...options,
        path: `${path}.${element["name"]}`,
        contains: complexTypeDefs[elementType]["contains"]
      });
    } else if (element["contains"]) {
      reverseReferences = getReverseReferences({
        ...options,
        path: `${path}.${element["name"]}`,
        contains: element["contains"]
      });
    }
  }
  return reverseReferences;
}
