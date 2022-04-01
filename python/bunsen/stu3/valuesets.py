"""
Support for broadcasting valuesets and using them in user-defined functions
in Spark queries.
"""

from collections import namedtuple

from bunsen.stu3.codes import get_value_sets, get_hierarchies, create_value_sets, create_hierarchies

# Placeholder record to load a particular value set
ValueSetPlaceholder = namedtuple("ValueSetPlaceholder",
                                 "valueSetUri valueSetVersion")

# Placeholder record to load a particular hierarchical system
HierarchyPlaceholder = namedtuple("HierarchyPlaceholder",
                                  "codeSystem codeValue hierarchyUri hierarchyVersion")

def valueset(valueset_uri, valueset_version):
    """
    Creates a placeholder specifying a specific valueset
    for use with :func:`push_valuesets`.

    :param valueset_uri: the URI of the valueset
    :param valueset_version: the version of the valueset
    :return: a placeholder for use with :func:`push_valuesets`
    """
    return ValueSetPlaceholder(valueset_uri, valueset_version)

def isa_loinc(code_value, loinc_version=None):
    """
    Returns a hierarchy placeholder that will load all values that are descendents
    of a given LOINC code.

    :param code_value: the parent code value
    :param loinc_version: the version of LOINC to use (uses latest if None is given)
    :return: a placeholder for use with :func:`push_valuesets`
    """
    return HierarchyPlaceholder('http://loinc.org',
                                code_value,
                                'urn:com:cerner:bunsen:hierarchy:loinc',
                                loinc_version)

def isa_snomed(code_value, snomed_version=None):
    """
    Returns a hierarchy placeholder that will load all values that are descendents
    of a given SNOMED code.

    :param code_value: the parent code value
    :param loinc_version: the version of SNOMED to use (uses latest if None is given)
    :return: a placeholder for use with :func:`push_valuesets`
    """
    return HierarchyPlaceholder('http://snomed.info/sct',
                                code_value,
                                'urn:com:cerner:bunsen:hierarchy:snomed',
                                snomed_version)

def push_valuesets(spark_session, valueset_map, database='ontologies'):
    """
    Pushes valuesets onto a stack and registers an in_valueset user-defined function
    that uses this content.

    The valueset_map takes the form of {referenceName: [(codeset, codevalue), (codeset, codevalue)]}
    to specify which codesets/values are used for the given valueset reference name.

    Rather than explicitly passing a list of (codeset, codevalue) tuples, users may instead
    load particular value sets or particular hierarchies by providing a ValueSetPlaceholder
    or HierarchyPlaceholder that instructs the system to load codes belonging to a particular
    value set or hierarchical system, respectively. See the isa_loinc and isa_snomed functions
    above for details.

    Finally, ontology information is assumed to be stored in the 'ontologies' database by
    default, but users can specify another database name if they have customized
    ontologies that are separated from the default ontologies database.

    :param spark_session: the SparkSession instance
    :param valueset_map: a map containing value set structures to publish
    :param database: the database from which value set data is loaded
    """

    loads_valuesets = False
    loads_hierarchies = False

    jvm = spark_session._jvm

    builder = jvm.com.cerner.bunsen.spark.codes.broadcast.BroadcastableValueSets.newBuilder()

    for (name, content) in valueset_map.items():

        if type(content) is HierarchyPlaceholder:

            # Add codes belonging to the specified hierarchy
            (codeSystem, codeValue, hierarchyUri, hierarchyVersion) = content

            builder.addDescendantsOf(name,
                                     codeSystem,
                                     codeValue,
                                     hierarchyUri,
                                     hierarchyVersion)

            loads_hierarchies = True

        elif type(content) is ValueSetPlaceholder:

            # Add codes belonging to the specified value set
            (valueSetUri, valueSetVersion) = content

            builder.addReference(name, valueSetUri, valueSetVersion)

            loads_valuesets = True

        else:

            # Add the explicitly provided code values
            for (codeSystem, codeValue) in content:
                builder.addCode(name, codeSystem, codeValue)

    valuesets = get_value_sets(spark_session, database) if loads_valuesets else create_value_sets(spark_session)

    hierarchies = get_hierarchies(spark_session, database) if loads_hierarchies else create_hierarchies(spark_session)

    broadcastable = builder.build(spark_session._jsparkSession,
                                  valuesets._jvalue_sets,
                                  hierarchies._jhierarchies)

    jvm.com.cerner.bunsen.spark.ValueSetUdfs.pushUdf(spark_session._jsparkSession, broadcastable)

def get_current_valuesets(spark_session):
    """
    Returns the current valuesets in the same form that is accepted by
    the push_valuesets function above, that is the structure will follow this pattern:
    {referenceName: [(codeset, codevalue), (codeset, codevalue)]}

    :param spark_session: the SparkSession instance
    :return: a map containing the valuesets currently published to the cluster
    """
    jvm = spark_session._jvm

    current = jvm.com.cerner.bunsen.spark.ValueSetUdfs.currentValueSets()

    if current is None:
        return None
    else:
        valuesets = current.getValue()

        return {name: [(system, value)
                       for system in valuesets.getValues(name).keySet()
                       for value in valuesets.getValues(name).get(system)]
                for name in valuesets.getReferenceNames()}

def pop_valuesets(spark_session):
    """
    Pops the current valuesets from the stack, returning true if there remains
    an active valueset, or false otherwise.

    :param spark_session: the SparkSession instance
    """
    jvm = spark_session._jvm

    return jvm.com.cerner.bunsen.spark.ValueSetUdfs.popUdf(spark_session._jsparkSession)
