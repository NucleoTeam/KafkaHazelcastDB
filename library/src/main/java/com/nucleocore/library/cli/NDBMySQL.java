package com.nucleocore.library.cli;

import java.io.*;
import java.net.*;

public class NDBMySQL{
    private final int port;

    public NDBMySQL(int port) {
        this.port = port;
    }

    public void start() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Simple MySQL Server started on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
            ) {
                String clientInput;
                while ((clientInput = reader.readLine()) != null) {
                    System.out.println("Received: " + clientInput);

                    if (clientInput.trim().toUpperCase().startsWith("SELECT")) {
                        out.println("id\tname");
                        out.println("1\tAlice");
                        out.println("2\tBob");
                        out.println("3\tCharlie");
                        out.println("Query OK, 3 rows in set");
                    } else {
                        out.println("Query OK, 0 rows affected");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        int port = 3307; // Change this to avoid conflicts with actual MySQL server
        NDBMySQL server = new NDBMySQL(port);
        server.start();
    }
}