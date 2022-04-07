from setuptools import setup

setup(
    name='pathling',
    packages=['pathling'],  # this must be the same as the name above
    version='1.0.0',
    description='Python API to Pathling',
    author='Piotr Szul',
    author_email='piotr.szul@csiro.au',
    url='https://github.com/aehrc/pathling',
    keywords=['testing', 'logging', 'example'],  # arbitrary keywords
    classifiers=[],
    license="CSIRO Open Source Software Licence Agreement",
    python_requires=">=3.7",
    #  install_requires=[
    #    'typedecorator==0.0.5'
    #  ],
    extras_require={
        'spark': [
            'pyspark>=3.2.1',
        ]
    },
)
