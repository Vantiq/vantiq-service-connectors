## Repository Contents

This repository contains an SDK for building Vantiq Service Connectors in Python. Service connectors allow some of all
of a Vantiq service to be implemented outside the Vantiq platform.  The typical use case for this is to leverage a language other than VAIl (in this case Python).  One major use case for Python is to take advantage of its broad support for ML/Generative AI libraries.

## Overview of a Service Connector

TBD...

## Developer Setup

In general, building, test execution, and publishing are all integrated with standard Gradle tasks defined by the parent project.  We use a Gradle Python plug-in to help with this integration.  The plug-in will construct a Python "virtual environment" into which all the necessary dependencies will be deployed.  This can be triggered directly using the `pipInstall` task.  The virtualenv is located in `./gradle/python`.

For anyone interested in working with the code in an IDE, we have found that the best option is to use PyCharm (community will work) to load this directory as a project (rather than via the parent project).  Then configure the python interpreter to point to the virtualenv created above and from there everything should work as expected.  Note that using the standard IDE (at least the community edition) does not fully work as it seems not to recognize the paths set up for pytest.