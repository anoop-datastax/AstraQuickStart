package com.datastax.quickstart;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import java.nio.file.Paths;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class AstraGettingStartedInsert {

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            // Load astra.properties from file
            props.load(new FileInputStream("src/main/resources/astra.properties"));
        } catch (IOException e) {
            System.out.println("Could not load astra.properties file.");
            e.printStackTrace();
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

            for (int i = 0; i < 1000; i++) {
                setUser(session, "lastname" + i, 30 + (i % 10), "City" + (i % 100), "email" + i + "@example.com", "Firstname" + i);
            }

            getUser(session, "lastname0");
        }
    }

    private static void setUser(CqlSession session, String lastname, int age, String city, String email, String firstname) {
        SimpleStatement stmt = SimpleStatement.builder("INSERT INTO users (lastname, age, city, email, firstname) VALUES (?, ?, ?, ?, ?)")
                .addPositionalValues(lastname, age, city, email, firstname)
                .build();
        session.execute(stmt);
        System.out.println("User " + firstname + " added.");
    }

    private static void getUser(CqlSession session, String lastname) {
        SimpleStatement stmt = SimpleStatement.builder("SELECT firstname, age FROM users WHERE lastname = ?")
                .addPositionalValue(lastname)
                .build();
        ResultSet rs = session.execute(stmt);
        Row row = rs.one();
        if (row != null) {
            System.out.println("Firstname: " + row.getString("firstname"));
            System.out.println("Age: " + row.getInt("age"));
        } else {
            System.out.println("User not found.");
        }
    }

    // Removed updateUser and deleteUser methods as they are not needed
}
