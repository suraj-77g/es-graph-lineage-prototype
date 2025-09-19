# Elasticsearch Graph Lineage Prototype

This project is a simple Java application to test the performance of large-scale graph traversals (like data lineage) using Elasticsearch's standard Search API.
It allows you to generate and index millions of unique graph edges and then run multi-hop queries to measure latency.

Prerequisites
Java 11+
Maven 3.6+
Docker & Docker Compose

### Quickstart
1. Start Elasticsearch
   Run the provided Elasticsearch 7.17.14 container.

docker-compose up -d

2. Build the Project
   Compile the Java code and package it into an executable JAR file.
   mvn clean package

3. Index Sample Data
   Use the index command to generate and load data.
   Syntax: java -jar ... index <num_nodes> <num_edges>

   Example: Create a graph with 200,000 nodes and 1,000,000 unique edges
   java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar index 200000 1000000

4. Run a Lineage Query
   Use the query command to perform a graph traversal.
   Syntax: java -jar ... query <start_urn> <depth>
   Example: Find a 3-hop downstream lineage starting from 'urn:node:123'
   java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar query urn:node:123 3

Troubleshooting
If Elasticsearch fails to start with a data path is not compatible error, it means an old Docker volume from a different ES version exists. To fix this:
* Stop the container: docker-compose down
* Remove the old volume: docker volume rm es-graph-prototype_esdata
* Restart the container: docker-compose up -d
