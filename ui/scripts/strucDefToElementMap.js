/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

process.stdin.setEncoding("utf8");

let data = "";

process.stdin.on("readable", () => {
  const chunk = process.stdin.read();
  if (chunk !== null) {
    data += chunk;
  }
});

process.stdin.on("end", () => {
  const bundle = JSON.parse(data);
  const entryMap = bundle["entry"].reduce(
    (acc, entry) => ({
      ...acc,
      ...transformStrucDefToElementMap(entry["resource"])
    }),
    {}
  );
  process.stdout.write(JSON.stringify(entryMap, null, 2));
});

function transformStrucDefToElementMap(strucDef) {
  const elementMap = {};
  for (const element of strucDef["snapshot"]["element"]) {
    const paths = element["path"].match(/\[x]$/)
      ? extractPathsFromPolyElement(element)
      : extractPathsFromElement(element);

    for (const path of paths) {
      const components = path.path.split(".");
      const resourceName = components[0];
      const tail = components.slice(1);

      if (components.length === 1) {
        elementMap[resourceName] = {
          ...elementMap[resourceName],
          definition: path.definition
        };
      } else if (components.length > 1) {
        const currentContains =
          resourceName in elementMap && elementMap[resourceName].contains
            ? elementMap[resourceName].contains
            : [];

        elementMap[resourceName] = {
          ...elementMap[resourceName],
          contains: mergePathIntoContains({
            path: tail,
            contains: currentContains,
            fhirPath: path.path,
            type: path.type,
            definition: path.definition,
            referenceTypes: path.referenceTypes
          })
        };
      }
    }
  }
  return elementMap;
}

function extractPathsFromElement(element) {
  let path = {
    path: element["path"],
    type: element["type"] ? element["type"][0]["code"] : "Resource",
    definition: element["definition"]
  };
  if (path.type === "Reference")
    path = extractReferenceTypesFromElement(path, element);
  return [path];
}

function extractReferenceTypesFromElement(path, element) {
  const type = element["type"].filter(t => t["code"] === "Reference")[0],
    referenceTypes = type["targetProfile"].map(t =>
      t.replace("http://hl7.org/fhir/StructureDefinition/", "")
    );
  return { ...path, referenceTypes };
}

function extractPathsFromPolyElement(element) {
  const paths = element["type"].map(t => ({
    path: element["path"].replace(
      /\[x]$/,
      t["code"].charAt(0).toUpperCase() + t["code"].slice(1)
    ),
    type: t["code"],
    definition: element["definition"]
  }));
  return paths.map(path =>
    path.type === "Reference"
      ? extractReferenceTypesFromElement(path, element)
      : path
  );
}

function mergePathIntoContains(path) {
  if (path.contains === undefined) path.contains = [];
  if (path.path.length < 1) return path.contains;
  const elementName = path.path[0];
  if (
    elementName === "id" ||
    elementName === "extension" ||
    elementName === "modifierExtension"
  )
    return [];
  const existingChild = path.contains.filter(c => c.name === elementName);
  const child = existingChild[0]
    ? existingChild[0]
    : {
        name: elementName,
        path: path.fhirPath,
        type: path.type,
        definition: path.definition,
        referenceTypes: path.referenceTypes
      };
  const newContains = mergePathIntoContains({
    ...path,
    path: path.path.slice(1),
    contains: child.contains
  });
  if (newContains.length > 0) child.contains = newContains;
  return path.contains.filter(c => c.name !== elementName).concat(child);
}
