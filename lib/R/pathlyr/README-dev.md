pathtlyr dev notes
================

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


### To do

Things todo and consider:

* Update README.md (possibly replace with README.Rmd to include alive code examples)
* Review the documentation and examples and intergate with pathling site.
* Minimize the size of example datasets (bundles in particular). Possibly generate as part of the build process for library-api and use here (it generates a NOTE for R CMD check)
* Add generation of conditions.rda from raw-data/conditions.csv
* Have separate github workflows/maven targets for building the package locally and building it for CRAN. In possibly consider Renv for local builds. CRAN build should not install spark (and thus run examples)
* Add vignettes with examples of using the API for particular use cases.
* And/or check The R Graphics Package (graphics) for the examples of how to include example code.
* Reconsider generation of 'DESCRIPTION' file. It's required very early for the package to be useable (e.g to install dependencies and or install the package from github). Possibly only modify in the snapshot build when needed.
* Consider not including the pathling jar in package distribution (optionally). The jar could be either explicitly installed/downloaded (e.g. like spark for sparklyr) or pulled from maven central. (Again there is a NOTE for this in R CMD check)



### Usefule resources:

- [R package development](https://r-pkgs.org/)
- [Tips on getting the package to CRAN](https://kbroman.org/pkg_primer/pages/cran.html)
- [Some more info on building R packages](https://www.paulamoraga.com/blog/2022/04/12/2022-04-12-rpackages/)
- [Roxygen2 quick reference](https://stuff.mit.edu/afs/athena/software/r/current/RStudio/resources/roxygen_help.html)
- [R github actions](https://github.com/r-lib/actions)
- [Renv](https://rstudio.github.io/renv/index.html)
