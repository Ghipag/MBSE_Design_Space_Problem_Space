# Knowledge Graph Representing the Design Exploration With MBSE Problem Space
This repository contains the code used to setup and interact with a knowledge graph covering current MBSE environment options and related issues and techniques for design space exploration. The companion paper to this repository is available [here]([https://ieeexplore.ieee.org/document/10521188](https://github.com/Ghipag/DRAGONS-development/tree/main)). Documentation covering the code itself may be found in /docs.

## Setup
The [Neo4j](https://neo4j.com/) database was setup to run inside a [docker container](https://neo4j.com/docs/operations-manual/current/docker/), and the python package [py2neo](https://py2neo.org/2021.1/) was used as an interface between the database and python. The knowledge graph data is stored under /data.
