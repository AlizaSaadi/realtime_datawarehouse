package test;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class Main1 {
    public static void main(String[] args) {
        BlockingQueue<String[]> rawQueue = new LinkedBlockingQueue<>();
        BlockingQueue<List<BlockingQueue<String[]>>> outerQueue = new LinkedBlockingQueue<>();
        BlockingQueue<List<String[]>> chunkQueue = new LinkedBlockingQueue<>(5);
        BlockingQueue<ConcurrentHashMap<String, String>> transformedQueue = new LinkedBlockingQueue<>();

        String firstCsvFilePath = "C:\\Users\\EeZee\\Downloads\\transactions_data.csv";
        String secondCsvFilePath = "C:\\Users\\EeZee\\Downloads\\customers_data.csv";
        String thirdCsvFilePath = "C:\\Users\\EeZee\\Downloads\\products_data.csv";
        long interval = 1000;

        List<String[]> customerData = readCsv(secondCsvFilePath);
        List<String[]> productData = readCsv(thirdCsvFilePath);

        List<String[]> crossJoinResult = crossJoinWithDetails(customerData, productData);
        List<List<String[]>> chunks = divideIntoChunks(crossJoinResult, 5);
        BlockingQueue<List<String[]>> circularQueue = new LinkedBlockingQueue<>(5);
        for (List<String[]> chunk : chunks) {
            try {
                circularQueue.put(chunk);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Prompt user for warehouse URL and password
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter warehouse URL: ");
        String url = scanner.nextLine();
        System.out.print("Enter warehouse user: ");
        String user = scanner.nextLine();
        System.out.print("Enter warehouse password: ");
        String password = scanner.nextLine();

        // Pass connection details to the consumer
        Thread producerThread = new Thread(new producer1(rawQueue, chunkQueue, firstCsvFilePath, interval));
        Thread transformerThread = new Thread(new transformer(chunkQueue, circularQueue, transformedQueue));
        Thread consumerThread = new Thread(new Consumer_Test(transformedQueue, url, user, password)); // Pass details here

        producerThread.start();
        transformerThread.start();
        consumerThread.start();
    }

    public static List<String[]> readCsv(String filePath) {
        List<String[]> data = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                data.add(line.split(",")); // Split the line by commas
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static List<String[]> crossJoinWithDetails(List<String[]> customerData, List<String[]> productData) {
        List<String[]> result = new ArrayList<>();
        List<String[]> customerDataWithoutHeader = customerData.subList(1, customerData.size());
        List<String[]> productDataWithoutHeader = productData.subList(1, productData.size());

        for (String[] customerRow : customerDataWithoutHeader) {
            for (String[] productRow : productDataWithoutHeader) {
                String[] combinedRow = new String[customerRow.length + productRow.length];
                System.arraycopy(customerRow, 0, combinedRow, 0, customerRow.length);
                System.arraycopy(productRow, 0, combinedRow, customerRow.length, productRow.length);
                result.add(combinedRow);
            }
        }
        return result;
    }

    public static List<List<String[]>> divideIntoChunks(List<String[]> data, int chunkCount) {
        List<List<String[]>> chunks = new ArrayList<>();
        int chunkSize = (int) Math.ceil((double) data.size() / chunkCount);

        for (int i = 0; i < chunkCount; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, data.size());
            chunks.add(new ArrayList<>(data.subList(start, end)));
        }

        return chunks;
    }
}
