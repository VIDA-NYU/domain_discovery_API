# Makefile for Domain Discovery API development
# Type "make" or "make all" to build the complete development environment
# Type "make help" for a list of commands

# Variables for the Makefile
.PHONY = conda_environment  word2vec_data clean
SHELL := /bin/bash
CONDA_ROOT := $(shell conda info --root)
CONDA_ENV := $(CONDA_ROOT)/envs/dd_api
#CONDA_ENV := $(CONDA_ROOT)/envs/ddt

CONDA_ENV_TARGET := $(CONDA_ENV)/conda-meta/history
DOWNLOADER_APP_TARGET := seeds_generator/target/seeds_generator-1.0-SNAPSHOT-jar-with-dependencies.jar
LINK_WORD2VEC_DATA_TARGET := ranking/D_cbow_pdw_8B.pkl
TSP_SOLVER_TARGET := ${PWD}/lib/tsp-solver-master/build

# Makefile commands, see below for actual builds

## all              : set up DD API development environment
all: conda_env downloader_app link_word2vec_data tsp_solver

## help             : show all commands.
# Note the double '##' in the line above: this is what's matched to produce
# the list of commands.
help                : Makefile
	@sed -n 's/^## //p' $<

## conda_env        : Install/update a conda environment with needed packages
conda_env: $(CONDA_ENV_TARGET)

## downloader_app   : Build the Java-based downloader application
downloader_app: $(DOWNLOADER_APP_TARGET)

## link_word2vec_data : Hardlink the word2vec data from the conda environment
link_word2vec_data: $(LINK_WORD2VEC_DATA_TARGET)

tsp_solver: $(TSP_SOLVER_TARGET)

# Actual Target work here

$(CONDA_ENV_TARGET): environment.yml
	conda env update

$(DOWNLOADER_APP_TARGET): $(CONDA_ENV_TARGET) seeds_generator/pom.xml $(wildcard seeds_generator/src/main/java/page_downloader/*.java)
	source activate dd_api; \
	pushd seeds_generator; \
	mvn compile assembly:single; \
	popd

$(TSP_SOLVER_TARGET): ${PWD}/lib/tsp-solver-master.zip
	source activate dd_api; \
  apt-get install unzip; \
	unzip ${PWD}/lib/tsp-solver-master.zip -d ${PWD}/lib; \
	pushd ${PWD}/lib/tsp-solver-master; \
	python setup.py install; \
	popd

$(LINK_WORD2VEC_DATA_TARGET): $(CONDA_ENV)/data/D_cbow_pdw_8B.pkl
	ln $(CONDA_ENV)/data/D_cbow_pdw_8B.pkl ${PWD}/ranking
