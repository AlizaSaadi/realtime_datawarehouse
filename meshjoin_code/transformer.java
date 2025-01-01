package test;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class transformer implements Runnable {
    private final BlockingQueue<List<String[]>> chunkQueue;
    private final BlockingQueue<List<String[]>> circularQueue;
    private final BlockingQueue<ConcurrentHashMap<String, String>> transformedQueue;

    public transformer(BlockingQueue<List<String[]>> chunkQueue, BlockingQueue<List<String[]>> circularQueue,
                       BlockingQueue<ConcurrentHashMap<String, String>> transformedQueue) {
        this.chunkQueue = chunkQueue;
        this.circularQueue = circularQueue;
        this.transformedQueue = transformedQueue;
    }

    @Override
    public void run() {
        try {
            //System.out.println("[Transformer] Starting transformation process.");

            while (true) {
                List<String[]> chunk = chunkQueue.take();
                //System.out.println("[Transformer] Processing chunk: " + chunk);
                
                List<String[]> topSegment = circularQueue.take();
                //System.out.println("[Transformer] Retrieved segment from circularQueue: " + topSegment);

                List<String[]> joinedData = join(topSegment, chunk);

                //mapping
                ConcurrentHashMap<String, String> resultMap = new ConcurrentHashMap<>();
                for (String[] row : joinedData) {
                    String compositeKey = row[0] + "_" + row[3]; // Modify indices as per your schema
                    resultMap.put(compositeKey, String.join(",", row));
                }

                transformedQueue.put(resultMap); 
                //System.out.println("[Transformer] Transformed data added to transformedQueue: " + resultMap);
                circularQueue.put(topSegment);
                //System.out.println("[Transformer] Circular queue rotated.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[Transformer] Thread interrupted.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String[]> join(List<String[]> segment, List<String[]> chunk) {
        List<String[]> joinedData = new ArrayList<>();

        for (String[] segmentRow : segment) {
            String segmentKey = segmentRow[0] + "_" + segmentRow[3]; 

            for (String[] chunkRow : chunk) {
                String chunkKey = chunkRow[4] + "_" + chunkRow[2]; 

                if (segmentKey.equals(chunkKey)) {
                    String[] joinedRow = new String[segmentRow.length + chunkRow.length];
                    System.arraycopy(segmentRow, 0, joinedRow, 0, segmentRow.length);
                    System.arraycopy(chunkRow, 0, joinedRow, segmentRow.length, chunkRow.length);

                    joinedData.add(joinedRow);
                }
            }
        }

        return joinedData;
    }
}
