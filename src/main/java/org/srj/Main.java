package org.srj;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String command = args[0];

        switch (command) {
            case "index":
                handleIndexCommand(args);
                break;
            case "query":
                handleQueryCommand(args);
                break;
            default:
                System.out.println("Unknown command: " + command);
                printUsage();
        }
    }

    private static void handleIndexCommand(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Invalid arguments for 'index' command.");
            printUsage();
            return;
        }
        try {
            int numDocs = Integer.parseInt(args[1]);
            int numUniqueNodes = Integer.parseInt(args[2]);
            Indexer indexer = new Indexer();
            indexer.generateAndIndexData(numDocs, numUniqueNodes);
            indexer.close();
        } catch (NumberFormatException e) {
            System.out.println("Error: <num_documents> and <num_unique_nodes> must be integers.");
            printUsage();
        }
    }

    private static void handleQueryCommand(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Invalid arguments for 'query' command.");
            printUsage();
            return;
        }
        try {
            String startUrn = args[1];
            int depth = Integer.parseInt(args[2]);
            Querier querier = new Querier();
            querier.findDownstreamLineage(startUrn, depth);
            querier.close();
        } catch (NumberFormatException e) {
            System.out.println("Error: <depth> must be an integer.");
            printUsage();
        }
    }

    private static void printUsage() {
        System.out.println("\n--- Elasticsearch Graph Lineage Prototype ---");
        System.out.println("Usage: java -jar <jar-file> [command] [options]");
        System.out.println("\nCommands:");
        System.out.println("  index <num_documents> <num_unique_nodes>");
        System.out.println("    - Generates and indexes graph data.");
        System.out.println("    - Example: java -jar prototype.jar index 1000000 10000");
        System.out.println("\n  query <start_urn> <depth>");
        System.out.println("    - Queries for downstream lineage from a starting node.");
        System.out.println("    - Example: java -jar prototype.jar query urn:node:1 2");
        System.out.println();
    }
}
