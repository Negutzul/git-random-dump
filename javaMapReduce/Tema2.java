import mapreduce.MapOperation;
import mapreduce.ReduceOperation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Tema2 {

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        int numberOfWorkers = Integer.parseInt(args[0]);
        String inputFile = args[1];
        String outputFile = args[2];

        int fragmentSize;
        int numberOfFiles;
        List<String> filenames = new ArrayList<>();

        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(inputFile))) {
            fragmentSize = Integer.parseInt(bufferedReader.readLine());
            numberOfFiles = Integer.parseInt(bufferedReader.readLine());
            for (int fileNumber = 0; fileNumber < numberOfFiles; ++fileNumber) {
                filenames.add(bufferedReader.readLine());
            }
        }

        List<MapOperation> mapOperations = new ArrayList<>();
        for (final String filename : filenames) {
            long fileSize = new File(filename).length();
            int offset;
            for (offset = 0; offset < fileSize - fragmentSize; offset += fragmentSize) {
                mapOperations.add(new MapOperation(filename, offset, fragmentSize));
            }
            if (offset + fragmentSize > fileSize) {
                mapOperations.add(new MapOperation(filename, offset, (int) (fileSize - offset)));
            }
        }

        List<MapWorkerThread> mapWorkerThreads = new ArrayList<>();
        for (int mapWorkerNumber = 0; mapWorkerNumber < numberOfWorkers; ++mapWorkerNumber) {
            List<MapOperation> mapOperationsForWorker = new ArrayList<>();
            for (int mapOperationIndex = mapWorkerNumber; mapOperationIndex < mapOperations.size(); mapOperationIndex += numberOfWorkers) {
                mapOperationsForWorker.add(mapOperations.get(mapOperationIndex));
            }

            MapWorkerThread mapWorkerThread = new MapWorkerThread(mapOperationsForWorker);
            mapWorkerThread.start();
            mapWorkerThreads.add(mapWorkerThread);
        }

        List<MapOperation.MapResult> mapResults =
            mapWorkerThreads
                .stream()
                .map(workerThread -> {
                    try {
                        workerThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return workerThread.getMapResults();
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        Map<String, List<MapOperation.MapResult>> mapResultsByFilename =
            mapResults
                .stream()
                .collect(Collectors.groupingBy(MapOperation.MapResult::getFilename));

        List<ReduceOperation> reduceOperations =
            mapResultsByFilename
                .entrySet()
                .stream()
                .map(filenameAndMapResults -> new ReduceOperation(filenameAndMapResults.getKey(), filenameAndMapResults.getValue()))
                .collect(Collectors.toList());

        List<ReduceWorkerThread> reduceWorkerThreads = new ArrayList<>();
        for (int reduceWorkerNumber = 0; reduceWorkerNumber < numberOfWorkers; ++reduceWorkerNumber) {
            List<ReduceOperation> reduceOperationsForWorker = new ArrayList<>();
            for (int reduceOperationIndex = reduceWorkerNumber; reduceOperationIndex < reduceOperations.size(); reduceOperationIndex += numberOfWorkers) {
                reduceOperationsForWorker.add(reduceOperations.get(reduceOperationIndex));
            }

            ReduceWorkerThread reduceWorkerThread = new ReduceWorkerThread(reduceOperationsForWorker);
            reduceWorkerThread.start();
            reduceWorkerThreads.add(reduceWorkerThread);
        }

        List<ReduceOperation.ReduceResult> reduceResults =
            reduceWorkerThreads
                .stream()
                .map(workerThread -> {
                    try {
                        workerThread.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return workerThread.getReduceResults();
                })
                .flatMap(Collection::stream)
                .sorted((firstReduceResult, secondReduceResult) -> Double.compare(secondReduceResult.getRank(), firstReduceResult.getRank()))
                .collect(Collectors.toList());

        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile))) {
            reduceResults.forEach(reduceResult -> {
                try {
                    bufferedWriter.write(
                        String.join(
                            ",",
                            reduceResult.getFilename().substring(1 + reduceResult.getFilename().lastIndexOf("/")),
                            String.format("%.2f", reduceResult.getRank()),
                            String.valueOf(reduceResult.getLongestWordLength()),
                            String.valueOf(reduceResult.getLongestWordCount())
                        ) + "\n"
                    );
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static class MapWorkerThread extends Thread {

        private final List<MapOperation> mapOperations;

        private List<MapOperation.MapResult> mapResults;

        public MapWorkerThread(List<MapOperation> mapOperations) {
            this.mapOperations = mapOperations;
        }

        public List<MapOperation.MapResult> getMapResults() {
            return mapResults;
        }

        @Override
        public void run() {
            mapResults =
                mapOperations
                    .stream()
                    .map(MapOperation::call)
                    .collect(Collectors.toList());
        }

    }

    private static class ReduceWorkerThread extends Thread {

        private final List<ReduceOperation> reduceOperations;

        private List<ReduceOperation.ReduceResult> reduceResults;

        public ReduceWorkerThread(List<ReduceOperation> reduceOperations) {
            this.reduceOperations = reduceOperations;
        }

        public List<ReduceOperation.ReduceResult> getReduceResults() {
            return reduceResults;
        }

        @Override
        public void run() {
            reduceResults =
                reduceOperations
                    .stream()
                    .map(ReduceOperation::call)
                    .collect(Collectors.toList());
        }

    }

}
