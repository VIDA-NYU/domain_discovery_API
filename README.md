# Domain Discovery (DD) API

The Domain Discovery API formalizes the human domain discovery process by defining a set of operations that capture the essential tasks that lead to domain discovery on the Web as we have discovered in interacting with the Subject Matter Experts (SME)s. 

## Installation

Building and deploying the Domain Discovery can be done using its Makefile to create a local development environment.  The conda build environment is currently only supported on 64-bit OS X and Linux.

### Local development

First install conda, either through the Anaconda or miniconda installers provided by Continuum.  You will also need Git, a Java Development Kit and Maven.  These are system tools that are generally not provided by conda.

Clone the DD API repository and enter it:

```
https://github.com/ViDA-NYU/domain_discovery_API
cd domain_discovery_API
```

Use the `make` command to build DDT and download/install its dependencies.

```
make
```

Now you can use the API

## Documentation

More documentation is available [HERE!](http://domain-discovery-api.readthedocs.io/en/latest/)

## Contact

Domain Discovery Development Team [ddt-dev@vgc.poly.edu]