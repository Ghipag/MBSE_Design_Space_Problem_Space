# Knowledge Graph Representing the Design Exploration With MBSE Problem Space
This repository contains the code used to setup and interact with a knowledge graph covering current MBSE environment options and related issues and techniques for design space exploration. The companion paper to this repository is currently in progress. Documentation covering the code itself may be found in /docs.

## Setup
The Neo4j database was setup to run inside a docker container, and the python package [py2neo](https://py2neo.org/2021.1/) was used as an interface between the database and python. THe data itself inn stored under /data