#!/usr/bin/env python

import os

from pathling import PathlingContext

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

# Read each Bundle into a row within a Spark data set.
bundles_dir = os.path.join(HERE, 'data/bundles/')
bundles = pc.spark.read.text(bundles_dir, wholetext=True)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode_bundle(bundles, 'Patient')

# JSON is the default format, XML Bundles can be encoded using input type.
# patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
