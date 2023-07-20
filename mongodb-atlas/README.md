# Storage Manager Service Connector for MongoDB Atlas

Leverages the Java storage manager SDK to build a connector for your MongoDB Atlas project.

The connector implements all entry points in the storage manager service API. It handles each request in an asynchronous
fashion returning results as Rx Observables. This approach ensures that any one request does not tie up the Vert.x
event loop thread for too long a time and lends itself to maximizing the utilization the underlying vCPU.

The underlying requests to MongoDB Atlas may have high latency values depending
on how far away the Atlas cluster resides in terms of network topology. MongoDB strongly recommends that you run your
Atlas clients in the same region (e.g. AWS U.S. East) as your Atlas cluster to minimize these latencies. The connector
does cache connections to minimize the overhead of initial handshaking / authentication.

## Connections

There are a couple of properties files that you need to configure before you can run the connector. The first is
config.properties. The connector looks here for property settings for the default Atlas database (cluster0).

The second properties file is secrets.properties. This file contains the username and password for authenticating to
Atlas. This file is useful for testing external storage manager service connectors only. It should never be included
in Docker images and never checked in to source code control. 

### Production Secret Handling

The java-rx-smgrsdk library has support for reading secrets mounted in a running container's file system. It works in
concert with the Vantiq server's configuring of secrets as Kubernetes secrets resources. Secrets are mounted in a
well-known location where they are picked up by the InstanceConfigUtils class.

### Start Up and Multi-Threading

On start up the connector will determine how many CPUs are available to it. It will then create 4 verticles for each
one. Vert.x assigns every 2 verticles to one event loop thread. Each verticle is capable of receiving and processing
requests concurrent to the other verticle sharing its event loop and in parallel with verticles on other event loops.
If for example you have 4 vCPUs, then you will have 8 event loop threads and 16 verticles.