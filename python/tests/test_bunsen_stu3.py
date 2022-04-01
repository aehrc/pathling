import os
import sys
import logging

from tempfile import mkdtemp
from pytest import fixture

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from bunsen.codes.loinc import with_loinc_hierarchy
from bunsen.codes.snomed import with_relationships
from bunsen.stu3.codes import create_concept_maps, get_concept_maps, create_value_sets, get_value_sets, create_hierarchies
from bunsen.stu3.bundles import load_from_directory, extract_entry, to_bundle, from_xml, from_json
from bunsen.stu3.valuesets import push_valuesets, isa_loinc, isa_snomed, get_current_valuesets

import xml.etree.ElementTree as ET

EXPECTED_COLUMNS = {'uri',
                    'version',
                    'descendantSystem',
                    'descendantValue',
                    'ancestorSystem',
                    'ancestorValue'}

@fixture(scope="module")
def spark_session(request):
  """
  Fixture for creating a Spark Session available for all tests in this
  testing session.
  """

  gateway_log = logging.getLogger('java_gateway')
  gateway_log.setLevel(logging.ERROR)

  # Get the shaded JAR for testing purposes.
  shaded_jar =  os.environ['SHADED_JAR_PATH']

  # Following the pattern in find_spark_home.py in Spark itself, this sets
  # the SPARK_HOME environment variable so the full Spark system is available
  # for tests.
  if not ('SPARK_HOME' in os.environ):
    if sys.version < "3":
      import imp
      os.environ['SPARK_HOME'] = imp.find_module("pyspark")[1]
    else:
      from importlib.util import find_spec
      os.environ['SPARK_HOME'] = os.path.dirname(find_spec("pyspark").origin)

  spark = SparkSession.builder \
    .appName('bunsen-test') \
    .master('local[2]') \
    .config('spark.jars', shaded_jar) \
    .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
    .config('spark.sql.warehouse.dir', mkdtemp()) \
    .config('javax.jdo.option.ConnectionURL',
            'jdbc:derby:memory:metastore_db;create=true') \
    .enableHiveSupport() \
    .getOrCreate()

  request.addfinalizer(lambda: spark.stop())

  return spark


# Concept Maps Tests
def test_with_new_map(spark_session):

  concept_maps = create_concept_maps(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  added = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  assert added.get_maps().count() == 1
  assert added.get_mappings().where(col('conceptmapuri') == 'urn:cerner:test:snomed-to-loinc').count() == 3

def test_add_mappings(spark_session):

  concept_maps = create_concept_maps(spark_session)

  original = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent')]

  added = [('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
           ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  appended = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=original) \
                         .add_mappings(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      new_version='0.2',
                                      mappings=added)

  assert appended.get_maps().count() == 2
  assert appended.get_mappings() \
      .where(col('conceptmapuri') == 'urn:cerner:test:snomed-to-loinc') \
      .where(col('conceptmapversion') == '0.1') \
      .count() == 1
  assert appended.get_mappings() \
      .where(col('conceptmapuri') == 'urn:cerner:test:snomed-to-loinc') \
      .where(col('conceptmapversion') == '0.2') \
      .count() == 3

def test_latest_concept_map_version(spark_session):

  concept_maps = create_concept_maps(spark_session)

  appended = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                       version='0.1',
                                       source='urn:cerner:test:valueset',
                                       target='http://hl7.org/fhir/ValueSet/observation-code',
                                       mappings=[]) \
                         .with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                       version='0.2',
                                       source='urn:cerner:test:valueset',
                                       target='http://hl7.org/fhir/ValueSet/observation-code',
                                       mappings=[])

  assert appended.latest_version('urn:cerner:test:snomed-to-loinc') == '0.2'

def test_with_maps_from_directory(spark_session):

  concept_maps = create_concept_maps(spark_session) \
      .with_maps_from_directory("tests/resources/conceptmaps")

  assert concept_maps.get_mappings("urn:cerner:poprec:fhir:conceptmap:demographics:gender", "0.0.1") \
      .count() == 5

def test_with_disjoint_maps_from_directory(spark_session):

  spark_session.sql("CREATE DATABASE IF NOT EXISTS disjoint_maps_db")

  create_concept_maps(spark_session) \
      .with_maps_from_directory("tests/resources/conceptmaps") \
      .write_to_database("disjoint_maps_db")

  reloaded = get_concept_maps(spark_session, "disjoint_maps_db") \
      .with_disjoint_maps_from_directory("tests/resources/conceptmaps", "disjoint_maps_db")

  assert reloaded.get_mappings("urn:cerner:poprec:fhir:conceptmap:demographics:gender", "0.0.1") \
      .count() == 5

def test_get_map_as_xml(spark_session):

  concept_maps = create_concept_maps(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  appended = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  xml_str = appended.get_map_as_xml('urn:cerner:test:snomed-to-loinc', '0.1')

  root = ET.fromstring(xml_str)
  assert root.tag == '{http://hl7.org/fhir}ConceptMap'

def test_write_maps(spark_session):

  concept_maps = create_concept_maps(spark_session)

  snomed_to_loinc = [('http://snomed.info/sct', '75367002', 'http://loinc.org', '55417-0', 'equivalent'), # Blood pressure
                     ('http://snomed.info/sct', '271649006', 'http://loinc.org', '8480-6', 'equivalent'), # Systolic BP
                     ('http://snomed.info/sct', '271650006', 'http://loinc.org', '8462-4', 'equivalent')] # Diastolic BP

  appended = concept_maps.with_new_map(url='urn:cerner:test:snomed-to-loinc',
                                      version='0.1',
                                      source='urn:cerner:test:valueset',
                                      target='http://hl7.org/fhir/ValueSet/observation-code',
                                      mappings=snomed_to_loinc)

  spark_session.sql('create database if not exists ontologies')
  spark_session.sql('drop table if exists ontologies.mappings')
  spark_session.sql('drop table if exists ontologies.ancestors')
  spark_session.sql('drop table if exists ontologies.conceptmaps')

  appended.write_to_database('ontologies')

  # Check that the maps were written by reloading and inspecting them.
  reloaded = get_concept_maps(spark_session)

  assert reloaded.get_maps().count() == 1
  assert reloaded.get_mappings().where(col('conceptmapuri') == 'urn:cerner:test:snomed-to-loinc').count() == 3

# Value Sets Tests
def test_add_valueset(spark_session):

  value_sets = create_value_sets(spark_session)

  values = [('urn:cerner:system1', 'urn:code:a'),
            ('urn:cerner:system1', 'urn:code:b'),
            ('urn:cerner:system2', 'urn:code:1')]

  appended = value_sets.with_new_value_set(url='urn:cerner:test:valuesets:testvalueset',
                                           version='0.1',
                                           values=values)

  assert appended.get_value_sets().count() == 1
  assert appended.get_values().count() == 3

def test_add_values(spark_session):

  value_sets = create_value_sets(spark_session)

  original = [('http://snomed.info/sct', '75367002')]

  added = [('http://snomed.info/sct', '271649006')]

  appended = value_sets.with_new_value_set(url='urn:cerner:test:valuesets:testvalueset',
                                           version='0.1',
                                           values=original) \
                       .add_values(url='urn:cerner:test:valuesets:testvalueset',
                                   version='0.1',
                                   new_version='0.2',
                                   values=added)

  assert appended.get_values().count() == 3
  assert appended.get_values() \
      .where(col('valueseturi') == 'urn:cerner:test:valuesets:testvalueset') \
      .where(col('valuesetversion') == '0.1') \
      .count() == 1
  assert appended.get_values() \
      .where(col('valueseturi') == 'urn:cerner:test:valuesets:testvalueset') \
      .where(col('valuesetversion') == '0.2') \
      .count() == 2

def test_latest_valueset_version(spark_session):

  value_sets = create_value_sets(spark_session)

  added = value_sets.with_new_value_set(url='urn:cerner:test:valuesets:testvalueset',
                                        version='0.1',
                                        values=[]) \
                    .with_new_value_set(url='urn:cerner:test:valuesets:testvalueset',
                                        version='0.2',
                                        values=[])

  assert added.latest_version('urn:cerner:test:valuesets:testvalueset') == '0.2'

def test_get_value_set_as_xml(spark_session):

  value_sets = create_value_sets(spark_session)

  values = [('urn:cerner:system1', 'urn:code:a'),
            ('urn:cerner:system1', 'urn:code:b'),
            ('urn:cerner:system2', 'urn:code:1')]

  appended = value_sets.with_new_value_set(url='urn:cerner:test:valuesets:testvalueset',
                                           version='0.1',
                                           values=values)
  # this test fails because version is null on line 778 of ValueSets.java
  xml_str = appended.get_value_set_as_xml('urn:cerner:test:valuesets:testvalueset', '0.1')

  root = ET.fromstring(xml_str)
  assert root.tag == '{http://hl7.org/fhir}ValueSet'

def test_with_value_sets_from_directory(spark_session):

  value_sets = create_value_sets(spark_session) \
      .with_value_sets_from_directory("tests/resources/valuesets")

  assert value_sets.get_values("urn:cerner:bunsen:valueset:married_maritalstatus", "0.0.1") \
      .count() == 1

def test_with_disjoint_value_sets_from_directory(spark_session):

  spark_session.sql("CREATE DATABASE IF NOT EXISTS disjoint_value_sets_db")

  create_value_sets(spark_session) \
      .with_value_sets_from_directory("tests/resources/valuesets") \
      .write_to_database("disjoint_value_sets_db")

  reloaded = get_value_sets(spark_session, "disjoint_value_sets_db") \
      .with_disjoint_value_sets_from_directory("tests/resources/valuesets", "disjoint_value_sets_db")

  assert reloaded.get_values("urn:cerner:bunsen:valueset:married_maritalstatus", "0.0.1") \
      .count() == 1

# LOINC Tests
def test_read_hierarchy_file(spark_session):
  ancestors = with_loinc_hierarchy(
      spark_session,
      create_hierarchies(spark_session),
      'tests/resources/LOINC_HIERARCHY_SAMPLE.CSV',
      '2.56').get_ancestors()

  assert set(ancestors.columns) == EXPECTED_COLUMNS

# SNOMED Tests
def test_read_relationship_file(spark_session):
  ancestors = with_relationships(
      spark_session,
      create_hierarchies(spark_session),
      'tests/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT',
      '20160901').get_ancestors()

  assert set(ancestors.columns) == EXPECTED_COLUMNS

# Bundles Tests
@fixture(scope="module")
def bundles(spark_session):
  return load_from_directory(spark_session, 'tests/resources/bundles/xml', 1)

def test_load_from_directory(bundles):
  assert bundles.count() == 3

def test_xml_bundles_from_df(spark_session):

  xml_bundles = spark_session.sparkContext.wholeTextFiles('tests/resources/bundles/xml').toDF()

  bundles = from_xml(xml_bundles, '_2')

  assert extract_entry(spark_session, bundles, 'Condition').count() == 5

def test_json_bundles_from_df(spark_session):

  json_bundles = spark_session.sparkContext.wholeTextFiles('tests/resources/bundles/json').toDF()

  bundles = from_json(json_bundles, '_2')

  assert extract_entry(spark_session, bundles, 'Condition').count() == 5

def test_extract_entry(spark_session, bundles):
  assert extract_entry(spark_session, bundles, 'Condition').count() == 5

def test_to_bundle(spark_session, bundles):
  conditions = extract_entry(spark_session, bundles, 'Condition')

  assert to_bundle(spark_session, conditions, 'Condition') != None

# ValueSetsUdfs Tests
def test_isa_loinc(spark_session):

  spark_session.sql('create database isa_loinc_ontologies')

  with_loinc_hierarchy(
      spark_session,
      create_hierarchies(spark_session),
      'tests/resources/LOINC_HIERARCHY_SAMPLE.CSV',
      '2.56') \
    .write_to_database('isa_loinc_ontologies')

  create_value_sets(spark_session).write_to_database('isa_loinc_ontologies')

  push_valuesets(spark_session,
                 {'leukocytes' : isa_loinc('LP14738-6')},
                 database='isa_loinc_ontologies')

  expected = {'leukocytes' : [('http://loinc.org', '5821-4'),
                              ('http://loinc.org', 'LP14738-6'),
                              ('http://loinc.org', 'LP14419-3')]}
  assert get_current_valuesets(spark_session) == expected

def test_isa_snomed(spark_session):

  spark_session.sql('create database isa_snomed_ontologies')

  with_relationships(
      spark_session,
      create_hierarchies(spark_session),
      'tests/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT',
      '20160901') \
    .write_to_database('isa_snomed_ontologies')

  create_value_sets(spark_session).write_to_database('isa_snomed_ontologies')

  push_valuesets(spark_session,
                 {'diabetes' : isa_snomed('73211009')},
                 database = 'isa_snomed_ontologies')

  expected = {'diabetes' : [('http://snomed.info/sct', '73211009'),
                            ('http://snomed.info/sct', '44054006')]}

  assert get_current_valuesets(spark_session) == expected

def test_isa_custom(spark_session, bundles):
  observations = extract_entry(spark_session, bundles, 'Observation')
  observations.registerTempTable('observations')

  blood_pressure = {'blood_pressure' : [('http://loinc.org', '8462-4')]}

  spark_session.sql('create database custom_ontologies')
  create_value_sets(spark_session).write_to_database('custom_ontologies')
  create_hierarchies(spark_session).write_to_database('custom_ontologies')

  push_valuesets(spark_session,
                 blood_pressure,
                 database='custom_ontologies')

  results = spark_session.sql("SELECT subject.reference, "
      + "effective.dateTime, "
      + "value.quantity.value "
      + "FROM observations "
      + "WHERE in_valueset(code, 'blood_pressure')")

  assert get_current_valuesets(spark_session) == blood_pressure
  assert results.count() == 14

def test_valueset_from_bundle(spark_session):
  bundles = load_from_directory(spark_session, 'tests/resources/bundles/json', 1)

  vs = extract_entry(spark_session, bundles, 'ValueSet')

  value_sets = create_value_sets(spark_session) \
    .with_value_sets(vs)

  assert value_sets.get_values("http://hl7.org/fhir/ValueSet/example-extensional", "20150622") \
    .count() == 4
