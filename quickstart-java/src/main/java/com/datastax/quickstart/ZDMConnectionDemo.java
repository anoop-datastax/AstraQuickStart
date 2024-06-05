package com.datastax.quickstart;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

public class ZDMConnectionDemo {

    public static void main(String[] args) {
        Properties props = new Properties();
        try {
            // Load cassandra.properties from file
            props.load(new FileInputStream("src/main/resources/cassandra.properties"));
        } catch (IOException e) {
            System.out.println("Could not load cassandra.properties file.");
            e.printStackTrace();
            return;
        }

        String contactPoint = props.getProperty("cassandra.contactPoint");
        int port = Integer.parseInt(props.getProperty("cassandra.port"));
        String userId = props.getProperty("cassandra.userId");
        String password = props.getProperty("cassandra.password");
        String keyspace = props.getProperty("cassandra.keyspace");

        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(contactPoint, port))
                .withAuthCredentials(userId, password)
                .withKeyspace(keyspace)
                .withLocalDatacenter("datacenter1")
                .build()) {

            setUser(session, "Jones", 35, "Austin", "bob@example.com", "Bob");

            getUser(session, "Jones");

            updateUser(session, 36, "Jones");

            getUser(session, "Jones");

            //deleteUser(session, "Jones");

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

    private static void updateUser(CqlSession session, int age, String lastname) {
        SimpleStatement stmt = SimpleStatement.builder("UPDATE users SET age = ? WHERE lastname = ?")
                .addPositionalValues(age, lastname)
                .build();
        session.execute(stmt);
        System.out.println("User with lastname " + lastname + " updated.");
    }

    private static void deleteUser(CqlSession session, String lastname) {
        SimpleStatement stmt = SimpleStatement.builder("DELETE FROM users WHERE lastname = ?")
                .addPositionalValue(lastname)
                .build();
        session.execute(stmt);
        System.out.println("User with lastname " + lastname + " deleted.");
    }
}
