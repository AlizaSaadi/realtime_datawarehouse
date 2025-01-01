package test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.BlockingQueue;
import java.util.ArrayList;
import java.util.List;

public class producer1 implements Runnable {

    private final BlockingQueue<String[]> queue;          
    private final BlockingQueue<List<String[]>> chunkQueue; 
    private final String csvFilePath;
    private final long interval;

    public producer1 (BlockingQueue<String[]> queue, BlockingQueue<List<String[]>> chunkQueue, String csvFilePath, long interval) {
        this.queue = queue;
        this.chunkQueue = chunkQueue;
        this.csvFilePath = csvFilePath;
        this.interval = interval;
    }

    @Override
    public void run() {
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            List<String[]> chunk = new ArrayList<>(); 

            //System.out.println("[Producer] Starting to read the file: " + csvFilePath);

            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                queue.put(data); 
                //System.out.println("[Producer] Added row to main queue: " + String.join(",", data));

                chunk.add(data);
                if (chunk.size() == 5) { 
                    chunkQueue.put(new ArrayList<>(chunk)); 
                    //System.out.println("[Producer] Added chunk to chunkQueue: " + chunk);
                    chunk.clear();
                    Thread.sleep(interval); 
                }
            }

            if (!chunk.isEmpty()) {
                chunkQueue.put(new ArrayList<>(chunk));
                //System.out.println("[Producer] Added final chunk to chunkQueue: " + chunk);
            }

            //System.out.println("[Producer] Completed processing the file.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
