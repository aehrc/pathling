Python API for Pathling
=======================

This is the Python API for [Pathling](https://pathling.csiro.au/) FHIR based analytics servers. 
It currently supporta encoding of JSON bundles and resources into Apache Spark dataframes. 

## Installation

Prerequisites: 
- Python 3.8+ with pip 
- PySpark 3.1+
 
Installation:
 
    pip install pathling  
    
## Code Usage

The code below shows an example of using Pathling API to encode Patient resources from JSON bundles:

    from pyspark.sql import SparkSession
    
    from pathling.r4 import bundles
    from pathling.etc import find_jar

    spark = SparkSession.builder \
        .appName('pathling-test') \
        .master('local[2]') \
        .config('spark.jars', find_jar()) \
        .getOrCreate()
                
    json_bundles = bundles.load_from_directory(spark, 'examples/data/bundles/R4/json/')
    patients = bundles.extract_entry(spark, json_bundles, 'Patient')
    patients.show()
    
More usage examples can be found in the `examples` directory.

## Development setup

Create an isolated python environment with `miniconda3`, e.g:

    conda create -n pathling-dev python=3.8
    conda activate   pathling-dev

To configure the environment for pathling development use:

    mvn -f ../../fhir-server/ -DskipTests=true -Dspring-boot.repackage.skip=true clean install
    mvn clean compile
    pip install -r ../../dev/dev-requirements.txt
    pip install -e .
    
To run the test and build the distribution package use:

    mvn clean package
    
