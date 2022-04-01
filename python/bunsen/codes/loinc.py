"""
Support for importing the LOINC Hierarchy into Bunsen.
"""

from bunsen.codes import Hierarchies

def with_loinc_hierarchy(sparkSession, hierarchies, loinc_hierarchy_path, loinc_version):
    """
    Returns a hierarchies instance that includes the LOINC hierarchy read
    from the given location.

    :param sparkSession: the spark session
    :param hierarchies: the :class:`bunsen.codes.Hierarchies` class
        to which the LOINC hierarchy should be added.
    :param loinc_hierarchy_path: the path of the LOINC hierarchy to load.
        This can be any path compatible with Hadoop's FileSystem API.
    :param loinc_version: the version of LOINC that is being loaded
    :return: a :class:`bunsen.codes.Hierarchies` with the added content.
    """
    loinc = sparkSession._jvm.com.cerner.bunsen.spark.codes.systems.Loinc

    jhierarchies = loinc.withLoincHierarchy(sparkSession._jsparkSession,
                                           hierarchies._jhierarchies,
                                           loinc_hierarchy_path,
                                           loinc_version)

    return Hierarchies(sparkSession, jhierarchies)
