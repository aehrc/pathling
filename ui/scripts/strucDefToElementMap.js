const mergePathIntoChildren = (path, children) => {
  if (children == undefined) children = []
  if (path.length < 1) return children
  const elementName = path[0]
  if (
    elementName == 'id' ||
    elementName == 'extension' ||
    elementName == 'modifierExtension'
  )
    return []
  const existingChild = children.filter(c => c.name == elementName)
  const child = existingChild[0] ? existingChild[0] : { name: elementName }
  const newChildren = mergePathIntoChildren(path.slice(1), child.children)
  if (newChildren.length > 0) child.children = newChildren
  return children.filter(c => c.name != elementName).concat(child)
}

const transformStrucDefToElementMap = strucDef => {
  const elementMap = {}
  for (const element of strucDef['snapshot']['element']) {
    const paths = element['path'].match(/\[x\]$/)
      ? element['type'].map(t =>
          element['path'].replace(
            /\[x\]$/,
            t['code'].charAt(0).toUpperCase() + t['code'].slice(1),
          ),
        )
      : [element['path']]
    for (const path of paths) {
      const components = path.split('.')
      const resourceName = components[0]
      const tail = components.slice(1)
      if (components.length > 1) {
        const currentChildren =
          resourceName in elementMap ? elementMap[resourceName] : []
        elementMap[resourceName] = mergePathIntoChildren(tail, currentChildren)
      }
    }
  }
  return elementMap
}

process.stdin.setEncoding('utf8')

let data = ''

process.stdin.on('readable', () => {
  const chunk = process.stdin.read()
  if (chunk !== null) {
    data += chunk
  }
})

process.stdin.on('end', () => {
  const bundle = JSON.parse(data)
  const entryMap = bundle['entry'].reduce(
    (acc, entry) => ({
      ...acc,
      ...transformStrucDefToElementMap(entry['resource']),
    }),
    {},
  )
  process.stdout.write(JSON.stringify(entryMap, null, 2))
})
