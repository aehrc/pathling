## Navigation model

FHIRPath navigates and selects nodes from a tree that abstracts away and
is independent of the actual underlying implementation of the source
against which the FHIRPath query is run. This way, FHIRPath can be used
on in-memory Plain Old Java Objects (POJOs), XML data or any other
physical representation, so long as that representation can be viewed as
classes that have properties. In somewhat more formal terms, FHIRPath
operates on a directed acyclic graph of classes as defined by a Meta
Object Facility (MOF)-equivalent [\[MOF\]](#MOF) type system. In this
specification, the structures on which FHIRPath operates are referred to
as the Object Model.

Data are represented as a tree of labelled nodes, where each node may
optionally carry a primitive value and have child nodes. Nodes need not
have a unique label, and leaf nodes must carry a primitive value. For
example, a (partial) representation of a FHIR Patient resource in this
model looks like this:

![Tree representation of a Patient](treestructure.png){height="375px"
width="500px" style="float: unset; margin-bottom:0;"}

The diagram shows a tree with a repeating `name` node, which represents repeating members of the FHIR
Object Model. Leaf nodes such as `use` and `family`
carry a (string) value. It is also possible for internal nodes to carry
a value, as is the case for the node labelled
`active`: this allows the tree
to represent FHIR \"primitives\", which may still have child extension
data.

FHIRPath expressions are then *evaluated* with respect to a specific
instance, such as the Patient one described above. This instance is
referred to as the *context* (also called the *root*) and paths within
the expression are evaluated in terms of this instance.