package com.workernode;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {

    public static void main(String[] args) {
        final ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
        System.out.println("Client for sending concurrent request to load balancer");
        Scanner input = new Scanner(System.in);
        System.out.println("Enter no of requests");
        int requestNo = Integer.parseInt(input.nextLine());
        System.out.println("Enter load balancer IP");
        String lbIP = input.nextLine();
        System.out.println("Enter load balancer Port");
        int port = Integer.parseInt(input.nextLine());

        for (int i = 0; i < requestNo; i++) {
            int finalI = i;
            executors.submit(() -> sendRequest(lbIP, port, finalI));
        }

        executors.shutdown();
    }

    private static void sendRequest(String ip, int port, int messageID) {
        try (Socket socket = new Socket(ip, port)) {
            if (socket.isConnected()) {
                Date date = new Date();
                long milliseconds = date.getTime();
                String data = milliseconds + messageID + "Message ";
                byte[] messageSent = prependLenBytes(data.getBytes());
                System.out.println("data sent is " + new String(messageSent));
                socket.getOutputStream().write(messageSent);

                socket.getOutputStream().flush();
                final byte[] lenBytes = new byte[2];
                socket.getInputStream().read(lenBytes);
                final int contentLength = bytesToShort(lenBytes);
                byte[] resp = new byte[contentLength];
                socket.getInputStream().read(resp);

                System.out.println("response for message ID " + messageID + " is " + new String(resp));

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static short bytesToShort(final byte[] bytes) {
        return ByteBuffer.wrap(bytes).getShort();
    }

    private static byte[] prependLenBytes(byte[] data) {
        short len = (short) data.length;
        byte[] newBytes = new byte[len + 2];
        newBytes[0] = (byte) (len / 256);
        newBytes[1] = (byte) (len & 255);
        System.arraycopy(data, 0, newBytes, 2, len);
        return newBytes;
    }
}
