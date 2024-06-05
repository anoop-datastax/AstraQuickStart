package com.datastax.quickstart;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.nio.file.Paths;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class QueryTable {

    public static void main(String[] args) {
        // Load properties from file
        Properties props = new Properties();
        try {
            props.load(new FileInputStream("src/main/resources/astra.properties"));
        } catch (IOException e) {
            System.out.println("Could not load properties file.");
            e.printStackTrace();
            return;
        }

        String secureConnectBundle = props.getProperty("astra.secureConnectBundlePath");
        String clientId = props.getProperty("astra.clientId");
        String clientSecret = props.getProperty("astra.clientSecret");
        String keyspace = props.getProperty("astra.keyspace");

        // Connect to Astra DB
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get(secureConnectBundle))
                .withAuthCredentials(clientId, clientSecret)
                .withKeyspace(keyspace)
                .build()) {

            queryData(session);
        }
    }

    private static void queryData(CqlSession session) {
        String query = "SELECT * FROM demo2 " +
                "WHERE trade_date < '2024-04-01' AND trade_date >= '2023-04-01' " +
                "AND status IN ('VERIFIED', 'ACTIVE') " +
                "ALLOW FILTERING;";

        try {
            ResultSet rs = session.execute(query);
            for (Row row : rs) {
                String tradeId = row.getString("trade_id");
                // Assuming you want to print other fields, add them accordingly
                String bookName = row.getString("book_name");
                String status = row.getString("status");
                System.out.println("Trade ID: " + tradeId + ", Book Name: " + bookName + ", Status: " + status);
            }
        } catch (Exception e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }
}
