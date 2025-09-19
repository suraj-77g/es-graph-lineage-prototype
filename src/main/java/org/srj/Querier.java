package org.srj;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Querier {

    private final RestClient restClient;
    private static final String INDEX_NAME = "es-graph-test-index";
    // Set a high limit for the number of connections to retrieve per hop.
    private static final int MAX_RESULTS_PER_HOP = 10000;

    public Querier() {
        this.restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();
    }

    public void close() throws IOException {
        this.restClient.close();
    }

    public void findDownstreamLineage(String startUrn, int depth) throws IOException {
        System.out.printf("Querying for %d-hop downstream lineage from: %s\n\n", depth, startUrn);

        Set<String> frontier = new HashSet<>();
        frontier.add(startUrn);
        Set<String> allConnections = new HashSet<>();
        Set<String> visitedDestinations = new HashSet<>(); // To avoid re-querying nodes
        visitedDestinations.add(startUrn);

        long totalLatency = 0;

        for (int i = 0; i < depth; i++) {
            if (frontier.isEmpty()) {
                System.out.printf("Stopping at hop %d as no further nodes were found.\n", i + 1);
                break;
            }
            System.out.printf("--- Hop %d ---\n", i + 1);
            System.out.printf("Querying with %d source nodes...\n", frontier.size());

            long hopStartTime = System.nanoTime();
            Set<String> nextFrontier = querySingleHop(frontier, allConnections);
            long hopEndTime = System.nanoTime();

            long hopLatency = TimeUnit.NANOSECONDS.toMillis(hopEndTime - hopStartTime);
            totalLatency += hopLatency;

            // Prepare for the next hop
            nextFrontier.removeAll(visitedDestinations); // Avoid cycles and redundant work
            visitedDestinations.addAll(nextFrontier);

            System.out.printf("Hop latency: %d ms, Found %d new nodes for next hop.\n", hopLatency, nextFrontier.size());
            frontier = nextFrontier;
        }

        System.out.println("\n--- Final Results ---");
        System.out.printf("Total query latency across all hops: %d ms\n", totalLatency);
        System.out.printf("Found %d total connections:\n", allConnections.size());
        //allConnections.forEach(System.out::println);
        System.out.println("--------------------");
    }

    private Set<String> querySingleHop(Set<String> sourceUrns, Set<String> allConnections) throws IOException {
        Set<String> nextFrontier = new HashSet<>();

        String sourceUrnsJson = sourceUrns.stream()
                .map(urn -> "\"" + urn.replace("\"", "\\\"") + "\"")
                .collect(Collectors.joining(","));

        // Switched from the licensed Graph API to the standard Search API.
        // This is a more robust solution that does not require a commercial license.
        String jsonPayload = String.format("{" +
                "  \"_source\": [\"sourceUrn\", \"destinationUrn\"]," +
                "  \"size\": %d," +
                "  \"query\": {" +
                "    \"terms\": {" +
                "      \"sourceUrn\": [%s]" +
                "    }" +
                "  }" +
                "}", MAX_RESULTS_PER_HOP, sourceUrnsJson);

        Request request = new Request("POST", "/" + INDEX_NAME + "/_search");
        request.setJsonEntity(jsonPayload);

        Response response = restClient.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(responseBody);

        // Parse the standard search response structure
        JsonNode hits = root.path("hits").path("hits");
        if (hits.isArray() && !hits.isEmpty()) {
            for (JsonNode hit : hits) {
                JsonNode source = hit.path("_source");
                JsonNode sourceNode = source.path("sourceUrn");
                JsonNode targetNode = source.path("destinationUrn");

                if (sourceNode.isTextual() && targetNode.isTextual()) {
                    String connectionString = String.format("  %s -> %s",
                            sourceNode.asText(), targetNode.asText());
                    allConnections.add(connectionString);
                    nextFrontier.add(targetNode.asText());
                }
            }
        }
        return nextFrontier;
    }
}

