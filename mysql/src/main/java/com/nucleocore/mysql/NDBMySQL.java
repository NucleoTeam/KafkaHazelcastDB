package com.nucleocore.mysql;

import java.io.*;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.MessageDigest;
import java.util.Random;

public class NDBMySQL{
    private final int port;

    public NDBMySQL(int port) {
        this.port = port;
    }

    public void start() {

        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(port));
            System.out.println("Simple MySQL Server started on port " + port);
            while (true) {
                SocketChannel clientSocket = serverSocketChannel.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }





    public static void main(String[] args) {
        int port = 3307; // Change this to avoid conflicts with actual MySQL server
        NDBMySQL server = new NDBMySQL(port);
        server.start();
    }
}