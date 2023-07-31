pathtlyr
================

To be provided.


### Setting up dev environment

These are relevant for MacOD

The following packages are needed to generate pdf manuals:

    brew install basictex
    brew install freetype

The following packages (may) be needed to build build the dependecies of 'devtools'

    brew install harfbuzz fribidi
    brew install libjpeg
    brew install libtiff
    brew install libgit2


To setup the dev environment, run the following commands:

Use miniconda to install R in the activated dev environment:

    # run 'conda activate your-dev-env-name' first
    conda install -y -c conda-forge r-base=4.1.3

Install the `devtools` R package:

    Rscript --vanilla -e 'install.packages("devtools", repos="https://cloud.r-project.org/")'




    

