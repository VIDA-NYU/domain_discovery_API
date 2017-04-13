Installation
============

Building and deploying the Domain Discovery can be done using its Makefile to create a local development environment.  The conda build environment is currently only supported on 64-bit OS X and Linux.

First install conda, either through the Anaconda or miniconda installers provided by Continuum.  You will also need Git, a Java Development Kit and Maven.  These are system tools that are generally not provided by conda.

Clone the DD API repository and enter it:

>>> git clone https://github.com/ViDA-NYU/domain_discovery_API
>>> cd domain_discovery_API

Use the `make` command to build DD API and download/install its dependencies.

>>> make

Now you can use the API 
