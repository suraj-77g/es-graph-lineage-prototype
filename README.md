# Elasticsearch Graph Lineage Prototype

This project is a simple Java service designed to test the performance of Elasticsearch's Graph Explore API for data lineage use cases. It allows you to:

Generate and index a large, synthetic graph of millions of edges into Elasticsearch.

Run graph traversal queries to simulate finding downstream dependencies and measure query latency.

#### Prerequisites

Java 11 or higher

Apache Maven

Docker and Docker Compose

#### 1. Setup & Installation

#####    Step 1: Start Elasticsearch

   From the root of the project directory, start the Elasticsearch container using Docker Compose.

docker-compose up -d

This will start a single-node Elasticsearch cluster on localhost:9200. Wait about 30-60 seconds for it to become fully available.

##### Step 2: Build the Java Application

Use Maven to compile the code and package it into a single executable JAR file.

mvn clean package

This will create a file named es-graph-lineage-prototype-1.0-SNAPSHOT.jar in the target/ directory.

#### 2. Usage

   The application is controlled via command-line arguments.

##### Step 3: Index Sample Graph Data

To test performance, you first need to load the synthetic graph data. The index command generates and indexes documents, where each document represents a single directed edge (source -> destination).

Command Format:

java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar index <num_documents> <num_unique_nodes>

<num_documents>: Total number of edges to create (e.g., 1000000 for 1 million).

<num_unique_nodes>: The size of the pool of unique nodes (vertices) to create edges from.

Example: Index 2 million edges from a pool of 50,000 unique nodes

java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar index 2000000 50000

The application will create an index named es-graph-test-index and bulk-index the data, printing its progress.

##### Step 4: Query the Graph

Once indexing is complete, you can run traversal queries using the query command to find downstream lineage.

Command Format:

java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar query <start_urn> <depth>

<start_urn>: The URN of the node to start the traversal from (e.g., urn:node:1). The node must exist in the pool you generated.

<depth>: The number of "hops" or levels to traverse downstream.

Example: Find 2-hop downstream lineage for urn:node:123

java -jar target/es-graph-lineage-prototype-1.0-SNAPSHOT.jar query urn:node:123 2

The service will execute the query against the Elasticsearch Graph Explore API and print the latency, along with the connections it found.

#### How It Works

Data Model: Each document in Elasticsearch represents a single directed edge with two fields: sourceUrn and destinationUrn. Both are mapped as keyword for exact matching.

Query: The query uses the _graph/explore API. It starts from a given sourceUrn and explores connections by matching the destinationUrn of one document to the sourceUrn of another, effectively "hopping" through the graph.