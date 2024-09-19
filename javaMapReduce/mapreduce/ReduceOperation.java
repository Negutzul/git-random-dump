package mapreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class ReduceOperation implements Callable<ReduceOperation.ReduceResult> {

    private final String filename;

    private final List<MapOperation.MapResult> mapResults;

    public ReduceOperation(String filename, List<MapOperation.MapResult> mapResults) {
        this.filename = filename;
        this.mapResults = mapResults;
    }

    private static long fibonacci(int n) {
        long last = 1;
        long result = 1;
        while (--n > 0) {
            long next = result + last;
            last = result;
            result = next;
        }
        return result;
    }

    @Override
    public ReduceResult call() {
        List<String> longestWordsAggregated =
            mapResults
                .stream()
                .map(MapOperation.MapResult::getLongestWords)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        int longestWordLength =
            longestWordsAggregated
                .stream()
                .map(String::length)
                .max(Integer::compareTo)
                .orElse(0);

        int longestWordCount =
            (int) longestWordsAggregated
                .stream()
                .filter(word -> word.length() == longestWordLength)
                .count();

        Map<Integer, Integer> aggregatedWordSizeAndAppearanceCount = new HashMap<>();
        mapResults.forEach(mapResult -> mapResult.getWordSizeAndAppearanceCount().forEach(
            (wordSize, appearanceCount) -> {
                if (!aggregatedWordSizeAndAppearanceCount.containsKey(wordSize)) {
                    aggregatedWordSizeAndAppearanceCount.put(wordSize, appearanceCount);
                } else {
                    aggregatedWordSizeAndAppearanceCount.put(wordSize, aggregatedWordSizeAndAppearanceCount.get(wordSize) + appearanceCount);
                }
            })
        );

        long product =
            aggregatedWordSizeAndAppearanceCount
                .entrySet()
                .stream()
                .map(wordSizeAndAppearanceCount -> fibonacci(wordSizeAndAppearanceCount.getKey()) * wordSizeAndAppearanceCount.getValue())
                .reduce(Long::sum)
                .orElse(0L);

        int totalNumberOfWords =
            aggregatedWordSizeAndAppearanceCount
                .values()
                .stream()
                .reduce(Integer::sum)
                .orElse(0);

        double rank = 1.0 * product / totalNumberOfWords;

        return new ReduceResult(filename, rank, longestWordLength, longestWordCount);
    }

    public static class ReduceResult {

        private final String filename;
        private final double rank;
        private final int longestWordLength;
        private final int longestWordCount;

        public ReduceResult(String filename, double rank, int longestWordLength, int longestWordCount) {
            this.filename = filename;
            this.rank = rank;
            this.longestWordLength = longestWordLength;
            this.longestWordCount = longestWordCount;
        }

        public String getFilename() {
            return filename;
        }

        public double getRank() {
            return rank;
        }

        public int getLongestWordLength() {
            return longestWordLength;
        }

        public int getLongestWordCount() {
            return longestWordCount;
        }

    }

}
