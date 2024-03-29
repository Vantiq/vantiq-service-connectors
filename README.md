# vantiq-service-connectors

> Note:  Use of service connectors requires VANTIQ Version 1.36 or later.
> Please ensure that the VANTIQ instance with which you are working has been
> updated to this version.
 
# Repository Overview

This repository contains the source code for VANTIQ service connectors as well as SDKs for building these connectors.
VANTIQ service connectors provide a means by which VANTIQ services can leverage native language implementations.

In general a service connector can be arbitrary code written in support of any service defined in a VANTIQ system. At
present, we support two use case scenarios:

    - Python Service Connectors in support of interfacing with Large Language Models (LLMs) and related tools
    - Service Connectors that implement the Storage Manager API

The two cases aren't mutually exclusive and there are likely to be additional uses for connectors going forward. For
now, these two concurrent purposes result in two SDKs. The connectors built using them are typically targeted at either
one scenario or the other.

## Repository Contents & Conventions

The various directories within this repository represent either SDKs to build service connectors or service connectors themselves.
By convention, SDKs will end with `sdk`.

Each directory will contain a README (or README.md) file that describes the contents, and either contains or directs
the reader toward appropriate documentation. Each directory should contain a LICENSE (or LICENSE.md) file that outlines
the license for the code as well as any packages used.

Bugs, issues, or enhancement requests should be reported using the GitHub *issues* tab.
In any such report or request,
please make clear the service connector or SDK to which the issue applies.
Including the top level directory in the issue's `Title` is the most expeditious way of doing so.

Some service connectors present will be written and supported by VANTIQ; others are contributed by other parties.

In general, branches other than `main` are considered development or experimental branches. Modulo any warnings or
caveats in the README, things in the `main` branch should be considered usable.

The repository is set up to require reviews of pull requests (PRs) going into main.
The primary purpose here is to maintain some level of consistency.

# Python Service Connectors Overview

TBD

# Storage Manager Service Connectors Overview

VANTIQ storage manager service connectors support the creation of native storage managers.
Within a VANTIQ system, a *storage manager* is the means by which the VANTIQ system communicates with other storage systems.
Each VANTIQ storage manager references a service that provides the implementation of the storage manager API.
The service determines whether that implementation is provided via VAIL procedures, or an alternative provided by a service connector.

Please refer to the
[VANTIQ documentation on service connectors](https://api.vantiq.com/docs/system/storagemanagers/index.html#service-connectors)
for details.

## Overall Architecture

The VANTIQ system maintains storage managers for each of its type resources. The storage manager
is responsible for the persistence of data associated with the type resource. The storage manager also provides the
means by which the VANTIQ system can query the associated data. The storage manager for a type is set when the type is
initially defined. If not explicitly set, the storage manager defaults to the storage mechanism run internally to the
VANTIQ cluster (MongoDB as of this writing). Pluggable storage managers name an implementing service that provides
the muscle behind the storage manager API. This implementation can be via VAIL procedures, or by a service
connector. All types that leverage native language implementations are defined with an implementing service that
references a service connector. The data model manager is responsible for the interaction between the VANTIQ system
and storage manager implementations. It recognizes when the storage manager service refers to a service connector and
directs the request over a websocket to the running connector. It then processes the responses as appropriate.

At present the VANTIQ server initiates all requests for data involving the service connector. This will change as we add support for
running service connectors external to the cluster and behind firewalls. In that case, the service connector will need to
initiate contact to the VANTIQ system to establish a websocket connection.

There are several operations that define the storage manager API that the service connector may (or may not) implement:
These operations are analogs to the 
[storage manager API for VAIL](https://api.vantiq.com/docs/system/storagemanagers/index.html#storage-manager-service-api) and are as follows:

  - getTypeRestrictions - let the VANTIQ server know what type system features the storage manager supports
  - initializeTypeDefinition - perform any / all work needed when a new type is created.
  - typeDefinitionDeleted - perform any / all work needed when a type is deleted.

The rest of the calls correspond to the standard data manipulation operations supported on VANTIQ types:

  - update
  - insertMany
  - insert
  - count
  - select
  - selectOne
  - delete

What follows is a general description of how things work.  For details in Java, please see the [Java SDK](java-rx-smgrsdk/README.md).

## Messages

> Note:  When using the [Java SDK](java-rx-smgrsdk/README.md) you may not need this level of detail. The focus can stay on the
> implementation of the above storage manager API operations.

Messages are received using a *SvcConnSvrMessage*.
This message contains the following properties.

 - `requestId` – The ID of the request sent by the server. The response must contain this ID, so it can be correlated to
the request
 - `procName` – the name of the procedure in question (see above). The name is a "dotted pair" containing:
`<service name>.<procedure name>`
 - `params` – the underlying parameters appropriate for the procedure invocation

Response messages adopt the HTTP Codes by convention. Use 2xx for success, 400 or greater for errors.

### Overall Protocol

For the storage manager service connector requests always come from the VANTIQ server. Responses can be either a single
message (*SvcConnSvrResponse*) or a larger number of messages in the case of some select operations. The VANTIQ server
will continually process a series of response messages as being part of the select operation response until the
connector indicates there is no more data (via the `isEOF` field in the response).

## Creating the Storage Manager Service Connector

The wiring involved in service connector based type resources is a bit more complex than that for VAIL based types.
Prior to being able to define types managed by a native storage manager backed by the service connector,
you must first define and deploy the service connector itself. Once deployed, you can then define the type resource by
selecting the storage manager and indicating the name of the service connector.

For details on defining a storage manager service connector in the VANTIQ system, please refer to the 
[documentation on service connectors](https://api.vantiq.com/docs/system/storagemanagers/index.html#service-connectors).

Once the definition is added, the VANTIQ system schedules the deployment with Kubernetes. There will be a brief
period while the cluster is spinning up the resources where the service connector will not be available. This
starting state could last a few minutes. Once complete Kubernetes monitors the health of the service connector
automatically, and may force restarts of the associated POD if the liveness checks fail.

# Developer Notes

To develop or build within this environment, we use `gradle` to build.

Some of the connectors require other software to build.
Generally, these are things that are not available as downloads via `gradle` dependencies,
or are things that are specific to each use.

Building some of the connectors requires a Java compiler.  To build everything, you'll need at least Java 11.

## Building Docker Images

The connectors in this repository contain `gradle` tasks that can be used to build Docker Images for each connector and 
push those images to the developer's Registry. In order to use these tasks, the developer must include the
following configuration option in their `gradle.properties` file or on the gradle command line,
along with some optional parameters:

*   `dockerRegistry`: Required. The name of the registry to which the image should be pushed (i.e. `docker.io`,
`quay.io`, etc.).
Note that this is used in naming the image even if you do not request publishing.
*   `pathToRepo`: Required. The path to the docker repository. This is typically the `namespace` portion of registry
URIs that follow the `registry/namespace/repo:tag` structure, but each registry can vary, (i.e. `pathToRepo=/vantiq/`).
Note that here, too, this is used in naming the image even if you do not publish.
Generally, this must be numbers and lowercase letters, starting with a letter.
*   `dockerRegistryUser`: Optional. The username used for authenticating with the given docker registry.
If not provided, this will be set to the empty string.
If you are publishing, you will generally need this value.
*   `dockerRegistryPassword`: Optional. The password used for authenticating with the given docker registry.
If not provided, this will be set to the empty string.
If you are publishing, you will generally need this value.
*   `imageTag`: Optional. The tag used when pushing the image. If not specified, the tag will default to "latest".
*   `repositoryName`: Optional. The name of the repository in the registry to which the image should be pushed. If not
specified, the default repository will be the connector's name (i.e. "mongodb-atlas").
*   `connectorSpecificInclusions`: Optional. The path to a directory of files that need to be included in the image. 
These can then be referenced and used by the Dockerfile.

Note that the `repositoryName` and, most likely, `connectorSpecificInclusions`, will be most appropriate on the
gradle command line.
Otherwise, the `repositoryName` will be used for all connectors built,
and that will be overwritten by the last one built.

With the required properties in place, the tasks can then be executed as follows:

### Build Image

From the root directory of this repo, run the following command (this example builds the MongoDB Atlas Connector)
```
./gradlew mongodb-atlas:buildServiceConnectorImage -PrepositoryName=mongodb-service-connector
```

### Push Image

From the root directory of this repo, run the following command (this example pushes the MongoDB Atlas Connector image)
```
./gradlew mongodb-atlas:pushServiceConnectorImage -PrepositoryName=mongodb-service-connector
```

### Deploying the connector images in the Vantiq IDE

Once you have built and published the docker image for a given connector (as described above), you can then deploy it 
into the Vantiq Cluster directly from the Vantiq IDE.

For a Storage Manager Service Connector this process is described
[here](https://dev.vantiq.com/docs/system/storagemanagers/index.html#service-connectors).