"""
Support for importing SNOMED relationship files into Bunsen.
"""

from bunsen.codes import Hierarchies

def with_relationships(sparkSession, hierarchies, snomed_relationship_path, snomed_version):
    """
    Returns a hierarchies instance that includes the SNOMED relationships read
    from the given location.

    :param sparkSession: the spark session
    :param hierarchies: the :class:`bunsen.codes.Hierarchies` class
        to which the SNOMED hierarchy should be added.
    :param snomed_relationship_path: the path of the SNOMED file to load.
        This can be any path compatible with Hadoop's FileSystem API.
    :param snomed_version: the version of SNOMED that is being loaded
    :return: a :class:`bunsen.codes.Hierarchies` with the added content.
    """
    snomed = sparkSession._jvm.com.cerner.bunsen.spark.codes.systems.Snomed

    jhierarchies = snomed.withRelationships(sparkSession._jsparkSession,
                                           hierarchies._jhierarchies,
                                           snomed_relationship_path,
                                           snomed_version)

    return Hierarchies(sparkSession, jhierarchies)
