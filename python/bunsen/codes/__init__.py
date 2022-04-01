"""
Core library for working with `Concept Maps <https://www.hl7.org/fhir/conceptmap.html>`_
and `Value Sets <https://www.hl7.org/fhir/valueset.html>`_, and hierarchical code systems
in Bunsen. See the :py:class:`~bunsen.codes.ConceptMaps` class,
:py:class:`~bunsen.codes.ValueSets` class, and :py:class:`~bunsen.codes.Hierarchies`
class for details.
"""

from pyspark.sql import functions, DataFrame
import collections
import datetime

def _add_mappings_to_map(jvm, concept_map, mappings, java_package):
    """
    Helper function to add a collection of mappings in the form of a list of
    [(source_system, source_value, target_system, target_value, equivalence)] tuples
    to the given concept map.
    """
    groups = collections.defaultdict(list)

    for (ss, sv, ts, tv, eq) in mappings:
        groups[(ss,ts)].append((sv,tv,eq))

    for (source_system, target_system), values in groups.items():
        group = concept_map.addGroup()

        group.setSource(source_system)
        group.setTarget(target_system)

        for (source_value, target_value, equivalence) in values:
            element = group.addElement()
            element.setCode(source_value)
            target = element.addTarget()
            target.setCode(target_value)

            if equivalence is not None:

                enumerations = java_package.Enumerations

                equivEnum = enumerations.ConceptMapEquivalence.fromCode(equivalence)

                target.setEquivalence(equivEnum)

def _add_values_to_value_set(jvm, value_set, values):
    """
    Helper function to add a collection of values in the form of a list of
    [(source, value)] tuples to the given value set.
    """
    inclusions = collections.defaultdict(list)

    for (s, v) in values:
        inclusions[s].append(v)

    for system, values in inclusions.items():
        inclusion = value_set.getCompose().addInclude()

        inclusion.setSystem(system)

        # FHIR expects a non-empty version, so we use the current datetime for
        # ad-hoc value sets
        version = datetime.datetime \
            .now() \
            .replace(microsecond=0) \
            .isoformat(sep=' ')
        inclusion.setVersion(version)

        for value in values:
            inclusion.addConcept().setCode(value)

class ConceptMaps(object):
    """
    An immutable collection of FHIR Concept Maps to be used to map value sets.
    These instances are typically created via the :py:module `bunsen.codes.stu3`
    """

    def __init__(self, spark_session, jconcept_maps, jfunctions, java_package):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jconcept_maps = jconcept_maps
        self._jfunctions = jfunctions
        self._java_package = java_package

    def latest_version(self, url):
        """
        Returns the latest version of a map, or None if there is none."

        :param url: the URL identifying a given concept map
        :return: the version of the given map
        """
        df = self.get_maps().where(functions.col('url') == url)
        results = df.groupBy('version') \
          .agg(functions.max('version').alias('latest')) \
          .collect()
        return results[0].latest if len(results) > 0 else None

    def get_maps(self):
        """
        Returns a dataset of FHIR ConceptMaps without the nested mapping content,
        allowing users to explore mapping metadata.

        The mappings themselves are excluded because they can become quite large,
        so users should use the get_mappings method to explore a table of them.

        :return: a DataFrame of FHIR ConceptMap resources managed by this object
        """
        return DataFrame(self._jconcept_maps.getMaps(), self._spark_session._wrapped)

    def get_mappings(self, url=None, version=None):
        """
        Returns a dataset of all mappings which may be filtered by an optional
        concept map url and concept map version.

        :param url: Optional URL of the mappings to return
        :param version: Optional version of the mappings to return
        :return: a DataFrame of mapping records
        """
        df = DataFrame(self._jconcept_maps.getMappings(), self._spark_session._wrapped)

        if url is not None:
            df = df.where("conceptmapuri = '" + url + "'")

        if version is not None:
            df = df.where("conceptmapversion = '" + version + "'")

        return df

    def get_map_as_xml(self, url, version):
        """
        Returns an XML string containing the specified concept map.

        :param url: URL of the ConceptMap to return
        :param version: Version of the ConceptMap to return
        :return: a string containing the ConceptMap in XML form
        """
        concept_map = self._jconcept_maps.getConceptMap(url, version)
        return self._jfunctions.resourceToXml(concept_map)

    def with_new_map(self,
                     url,
                     version,
                     source,
                     target,
                     experimental=True,
                     mappings=[]):
        """
        Returns a new ConceptMaps instance with the given map added. Callers
        may include a list of mappings tuples in the form of
        [(source_system, source_value, target_system, target_value, equivalence)].

        :param url: URL of the ConceptMap to add
        :param version: Version of the ConceptMap to add
        :param source: source URI of the ConceptMap
        :param target: target URI of the ConceptMap
        :param experimental: a Boolean variable indicating whether the map should be
            labeled as experimental
        :param mappings: A list of tuples representing the mappings to add
        :return: a :class:`ConceptMaps` instance with the added map
        """
        concept_map = self._java_package.ConceptMap()
        concept_map.setUrl(url)
        concept_map.setVersion(version)
        concept_map.setSource(self._java_package.UriType(source))
        concept_map.setTarget(self._java_package.UriType(target))

        if (experimental):
            concept_map.setExperimental(True)

        _add_mappings_to_map(self._jvm, concept_map,
                             mappings, self._java_package)

        map_as_list = self._jvm.java.util.Collections.singletonList(concept_map)

        return ConceptMaps(self._spark_session,
                           self._jconcept_maps.withConceptMaps(map_as_list),
                           self._jfunctions,
                           self._java_package)

    def with_maps_from_directory(self, path):
      """
      Returns a new ConceptMaps instance with all maps read from the given
      directory path. The directory may be anything readable from a Spark path,
      including local filesystems, HDFS, S3, or others.

      :param path: Path to directory containing FHIR ConceptMap resources
      :return: a :class:`ConceptMaps` instance with the added maps
      """
      maps = self._jconcept_maps.withMapsFromDirectory(path)

      return ConceptMaps(self._spark_session,
                         maps,
                         self._jfunctions,
                         self._java_package)

    def with_disjoint_maps_from_directory(self, path, database="ontologies"):
      """
      Returns a new ConceptMaps instance with all value sets read from the given
      directory path that are disjoint with value sets stored in the given
      database. The directory may be anything readable from a Spark path,
      including local filesystems, HDFS, S3, or others.

      :param path: Path to directory containing FHIR ConceptMap resources
      :param database: The database in which existing concept maps are stored
      :return: a :class:`ConceptMaps` instance with the added maps
      """
      maps = self._jconcept_maps.withDisjointMapsFromDirectory(path, database)

      return ConceptMaps(self._spark_session,
                         maps,
                         self._jfunctions,
                         self._java_package)

    def add_mappings(self, url, version, new_version, mappings):
        """
        Returns a new ConceptMaps instance with the given mappings added to an existing map.
        The mappings parameter must be a list of tuples of the form
        [(source_system, source_value, target_system, target_value, equivalence)].

        :param url: URL of the ConceptMap to add mappings to
        :param version: Version of the ConceptMap to add mappings to
        :param new_version: Version of the updated ConceptMap to which new mappings have been
            added
        :param mappings: A list of tuples representing the mappings to add
        :return: a :class:`ConceptMaps` instance with the added mappings
        """
        concept_map = self._jconcept_maps.getConceptMap(url, version)
        concept_map.setVersion(new_version)

        _add_mappings_to_map(self._jvm, concept_map, mappings, self._java_package)

        map_as_list = self._jvm.java.util.Collections.singletonList(concept_map)

        return ConceptMaps(self._spark_session,
                           self._jconcept_maps.withConceptMaps(map_as_list),
                           self._jfunctions,
                           self._java_package)

    def write_to_database(self, database):
        """
        Writes the mapping content to the given database, creating a mappings
        and conceptmaps table if they don't exist.

        :param database: the database to write the concept maps to
        """
        self._jconcept_maps.writeToDatabase(database)

class ValueSets(object):
    """
    An immutable collection of FHIR Value Sets to be used to for
    ontologically-based queries.
    """

    def __init__(self, spark_session, jvalue_sets, jfunctions, java_package):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jvalue_sets = jvalue_sets
        self._jfunctions = jfunctions
        self._java_package = java_package

    def latest_version(self, url):
        """
        Returns the latest version of a value set, or None if there is none.

        :param url: URL of the ValueSet to return
        :return: the version of the ValueSet, or None if there is none
        """
        df = self.get_value_sets().where(functions.col('url') == url)
        results = df.groupBy('version') \
          .agg(functions.max('version').alias('latest')) \
          .collect()
        return results[0].latest if len(results) > 0 else None

    def get_value_sets(self):
        """
        Returns a dataset of FHIR ValueSets without the nested value content,
        allowing users to explore value set metadata.

        The values themselves are excluded because they can be become quite
        large, so users should use the get_values method to explore them.

        :return: a dataframe of FHIR ValueSets
        """
        return DataFrame(self._jvalue_sets.getValueSets(), self._spark_session._wrapped)

    def get_values(self, url=None, version=None):
        """
        Returns a dataset of all values which may be filtered by an optional
        value set url and value set version.

        :param url: Optional URL of ValueSet to return
        :param version: Optional version of the ValueSet to return
        :return: a DataFrame of values
        """
        df = DataFrame(self._jvalue_sets.getValues(), self._spark_session._wrapped)

        if  url is not None:
            df = df.where("valueseturi = '" + url + "'")

        if version is not None:
            df = df.where("valuesetversion = '" + version + "'")

        return df

    def get_value_set_as_xml(self, url, version):
        """
        Returns an XML string containing the specified value set.

        :param url: URL of the ValueSet to return
        :param version: Version of the ValueSet to return
        :return: a string containing the ValueSet in XML form
        """
        value_set = self._jvalue_sets.getValueSet(url, version)
        return self._jfunctions.resourceToXml(value_set)

    def with_new_value_set(self,
                           url,
                           version,
                           experimental=True,
                           values=[]):
        """
        Returns a new ValueSets instance with the given value set added. Callers
        may include a list of value tuples in the form of [(system, value)].

        :param url: URL of the ValueSet to add
        :param version: Version of the ValueSet to add
        :param experimental: a Boolean variable indicating whether the ValueSet should be
            labeled as experimental
        :param values: A list of tuples representing the values to add
        :return: a :class:`ValueSets` instance with the added value set.
        """
        value_set = self._java_package.ValueSet()
        value_set.setUrl(url)
        value_set.setVersion(version)

        if (experimental):
            value_set.setExperimental(True)

        _add_values_to_value_set(self._jvm, value_set, values)

        value_set_as_list = self._jvm.java.util.Collections.singletonList(value_set)

        return ValueSets(self._spark_session,
                         self._jvalue_sets.withValueSets(value_set_as_list),
                         self._jfunctions,
                         self._java_package)

    def with_value_sets(self, df):
        """
        Returns a new ValueSets instance that includes the ValueSet FHIR
        resources encoded in the given Spark DataFrame.

        :param df: A Spark DataFrame containing the valueset FHIR resource
        :return: a :class:`ValueSets` instance with the added value sets
        """
        value_sets = self._jvalue_sets.withValueSets(df._jdf)

        return ValueSets(self._spark_session,
                         value_sets,
                         self._jfunctions,
                         self._java_package)

    def with_value_sets_from_directory(self, path):
      """
      Returns a new ValueSets instance with all value sets read from the given
      directory path. The directory may be anything readable from a Spark path,
      including local filesystems, HDFS, S3, or others.

      :param path: Path to directory containing FHIR ValueSet resources
      :return: a :class:`ValueSets` instance with the added value sets
      """
      value_sets = self._jvalue_sets.withValueSetsFromDirectory(path)

      return ValueSets(self._spark_session,
                       value_sets,
                       self._jfunctions,
                       self._java_package)

    def with_disjoint_value_sets_from_directory(self, path, database="ontologies"):
      """
      Returns a new ValueSets instance with all value sets read from the given
      directory path that are disjoint with value sets stored in the given
      database. The directory may be anything readable from a Spark path,
      including local filesystems, HDFS, S3, or others.

      :param path: Path to directory containing FHIR ValueSet resources
      :param database: The database in which existing value sets are stored
      :return: a :class:`ValueSets` instance with the added value sets
      """
      value_sets = self._jvalue_sets.withDisjointValueSetsFromDirectory(path, database)

      return ValueSets(self._spark_session,
                       value_sets,
                       self._jfunctions,
                       self._java_package)

    def add_values(self, url, version, new_version, values):
        """
        Returns a new ValueSets instance with the given values added to an
        existing value set. The values parameter must be a list of the form
        [(sytem, value)].

        :param url: URL of the ValueSet to add values to
        :param version: Version of the ValueSet to add values to
        :param new_version: Version of the updated ValueSet to which new values
            have been added
        :param mappings: A list of tuples representing the values to add
        :return: a :class:`ValueSets` instance with the added values
        """
        value_set = self._jvalue_sets.getValueSet(url, version)
        value_set.setVersion(new_version)

        _add_values_to_value_set(self._jvm, value_set, values)

        value_set_as_list = self._jvm.java.util.Collections.singletonList(value_set)

        return ValueSets(self._spark_session,
                         self._jvalue_sets.withValueSets(value_set_as_list),
                         self._jfunctions,
                         self._java_package)

    def write_to_database(self, database):
        """
        Writes the value set content to the given database, creating a values
        and valuesets table if they don't exist.

        :param database: the database to write the value sets to
        """
        self._jvalue_sets.writeToDatabase(database)

class Hierarchies(object):
    """
    An immutable collection of values from hierarchical code systems to be used
    for ontologically-based queries.
    """

    def __init__(self, spark_session, jhierarchies):
        self._spark_session = spark_session
        self._jvm = spark_session._jvm
        self._jhierarchies = jhierarchies

    def latest_version(self, uri):
        """
        Returns the latest version of a hierarchy, or None if there is none.

        :param uri: URI of the concept hierarchy to return
        :return: the version of the hierarchy, or None if there is none
        """
        df = get_ancestors().where(df.uri == functions.lit(uri))
        results = df.agg({"version": "max"}).collect()
        return results[0].min if results.count() > 0 else None

    def get_ancestors(self, url=None, version=None):
        """
        Returns a dataset of ancestor values representing the transitive
        closure of codes in this Hierarchies instance filtered by an optional
        hierarchy uri and version.

        :param url: Optional URL of hierarchy to return
        :param version: Optional version of the hierarchy to return
        :return: a DataFrame of ancestor records
        """
        df = DataFrame(self._jhierarchies.getAncestors(), self._spark_session._wrapped)

        if url is not None:
            df = df.where(df.uri == functions.lit(uri))

        if version is not None:
            df = df.where(df.version == functions.lit(veresion))

        return df

    def write_to_database(self, database):
        """
        Write the ancestor content to the given database, create an ancestors
        table if they don't exist.

        :param database: the database to write the hierarchies to
        """
        self._jhierarchies.writeToDatabase(database)
