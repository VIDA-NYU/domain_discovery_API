# domain_discovery_API

An API that contains domain discovery operations

## Installing on your machine

Building and deploying the Domain Discovery can be done using its Makefile to create a local development environment.  The conda build environment is currently only supported on 64-bit OS X and Linux.

### Local development

First install conda, either through the Anaconda or miniconda installers provided by Continuum.  You will also need Git and a Java Development Kit.  These are system tools that are generally not provided by conda.

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