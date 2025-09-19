package org.srj;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

public class Indexer {

    private final ElasticsearchClient esClient;
    private static final String INDEX_NAME = "es-graph-test-index";
    private static final int BATCH_SIZE = 5000;

    public Indexer() {
        RestClient restClient = RestClient.builder(new HttpHost("localhost", 9200)).build();
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        this.esClient = new ElasticsearchClient(transport);
    }

    public void close() throws IOException {
        if (this.esClient != null && this.esClient._transport() != null) {
            this.esClient._transport().close();
        }
    }

    public void generateAndIndexData(int numUniqueNodes, int numUniqueEdges) throws IOException {
        // Sanity check: It's impossible to connect N nodes with fewer than N-1 edges.
        if (numUniqueEdges < numUniqueNodes - 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot form a connected graph. Number of edges (%d) must be at least number of nodes - 1 (%d).",
                            numUniqueEdges, numUniqueNodes - 1)
            );
        }

        System.out.printf("Generating and indexing %d unique edges for %d unique nodes...\n", numUniqueEdges, numUniqueNodes);

        List<String> nodePool = new ArrayList<>(numUniqueNodes);
        for (int i = 0; i < numUniqueNodes; i++) {
            nodePool.add("urn:node:" + i);
        }

        Random random = new Random();
        Set<String> generatedEdges = new HashSet<>(numUniqueEdges);
        BulkRequest.Builder br = new BulkRequest.Builder();
        int generatedCount = 0;
        int currentBatchSize = 0;

        // --- Phase 1: Create a guaranteed connected graph by forming a random path ---
        System.out.println("Phase 1: Generating a spanning path to connect all nodes...");
        Collections.shuffle(nodePool, random); // Shuffle to create a random path

        for (int i = 0; i < numUniqueNodes - 1; i++) {
            String source = nodePool.get(i);
            String destination = nodePool.get(i + 1);
            String edgeKey = source + "->" + destination;

            if (generatedEdges.add(edgeKey)) {
                addDocToBulkRequest(br, source, destination);
                generatedCount++;
                currentBatchSize++;
                if (currentBatchSize == BATCH_SIZE) {
                    currentBatchSize = executeBulkRequest(br, generatedCount, numUniqueEdges);
                    br = new BulkRequest.Builder();
                }
            }
        }
        System.out.println("Phase 1 complete. All nodes are now guaranteed to be connected.");

        // --- Phase 2: Add the remaining edges randomly to increase graph complexity ---
        System.out.println("Phase 2: Generating remaining random edges...");
        while (generatedCount < numUniqueEdges) {
            String source = nodePool.get(random.nextInt(numUniqueNodes));
            String destination;
            do {
                destination = nodePool.get(random.nextInt(numUniqueNodes));
            } while (source.equals(destination));

            String edgeKey = source + "->" + destination;

            if (generatedEdges.add(edgeKey)) {
                addDocToBulkRequest(br, source, destination);
                generatedCount++;
                currentBatchSize++;
                if (currentBatchSize == BATCH_SIZE) {
                    currentBatchSize = executeBulkRequest(br, generatedCount, numUniqueEdges);
                    br = new BulkRequest.Builder();
                }
            }
        }
        System.out.println("Phase 2 complete.");

        if (currentBatchSize > 0) {
            System.out.printf("  ...indexing final batch of %d edges...\n", currentBatchSize);
            esClient.bulk(br.build());
        }

        System.out.printf("\nIndexing complete. Total unique edges indexed: %d\n", generatedCount);
    }

    private void addDocToBulkRequest(BulkRequest.Builder br, String source, String destination) {
        Map<String, String> doc = new HashMap<>();
        doc.put("sourceUrn", source);
        doc.put("destinationUrn", destination);
        br.operations(op -> op
                .index(idx -> idx
                        .index(INDEX_NAME)
                        .document(doc)
                )
        );
    }

    private int executeBulkRequest(BulkRequest.Builder br, int generatedCount, int totalEdges) throws IOException {
        System.out.printf("  ...indexed %d / %d edges...\n", generatedCount, totalEdges);
        BulkResponse result = esClient.bulk(br.build());
        if (result.errors()) {
            System.err.println("Bulk indexing had errors.");
            // Production code should handle errors more gracefully
        }
        return 0; // Reset batch size
    }
}

