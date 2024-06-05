package com.datastax.quickstart;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.nio.file.Paths;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.Properties;

public class AsyncQueryDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("src/main/resources/astra.properties"));
        } catch (Exception e) {
            System.out.println("Error loading properties file: " + e.getMessage());
            return;
        }

        String secureConnectBundle = props.getProperty("astra.secureConnectBundlePath");
        String clientId = props.getProperty("astra.clientId");
        String clientSecret = props.getProperty("astra.clientSecret");
        String keyspace = props.getProperty("astra.keyspace");

        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(secureConnectBundle))
                .withAuthCredentials(clientId, clientSecret)
                .withKeyspace(keyspace)
                .build()) {

            CompletableFuture<Void> verifiedFuture = executeQueryAsync(session, "VERIFIED")
                    .thenAccept(rows -> {
                        System.out.println("Verified Trades:");
                        rows.forEach(AsyncQueryDemo::printRow);
                    });

            CompletableFuture<Void> activeFuture = executeQueryAsync(session, "ACTIVE")
                    .thenAccept(rows -> {
                        System.out.println("Active Trades:");
                        rows.forEach(AsyncQueryDemo::printRow);
                    });

            // Combine both futures and handle their results when both complete
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(verifiedFuture, activeFuture);
            allFutures.join();  // Block until all queries are complete

        } catch (Exception e) {
            System.out.println("Error connecting to Cassandra: " + e.getMessage());
        }
    }

    private static CompletableFuture<List<Row>> executeQueryAsync(CqlSession session, String status) {
        String query = String.format("SELECT * FROM demo2 WHERE trade_date < '2024-04-01' AND trade_date >= '2023-04-01' AND status = '%s' ALLOW FILTERING;", status);
        return session.executeAsync(query)
                .thenCompose(AsyncQueryDemo::fetchAllRows).toCompletableFuture();
    }

    private static CompletableFuture<List<Row>> fetchAllRows(AsyncResultSet resultSet) {
        List<Row> results = new ArrayList<>();
        return fetchPage(results, resultSet);
    }

    private static CompletableFuture<List<Row>> fetchPage(List<Row> results, AsyncResultSet resultSet) {
        resultSet.currentPage().forEach(results::add);
        if (resultSet.hasMorePages()) {
            return (CompletableFuture<List<Row>>) resultSet.fetchNextPage().thenCompose(rs -> fetchPage(results, rs));
        } else {
            return CompletableFuture.completedFuture(results);
        }
    }

    private static void printRow(Row row) {
        System.out.println("Trade ID: " + row.getString("trade_id") + ", Book Name: " + row.getString("book_name") + ", Status: " + row.getString("status"));
    }
}
