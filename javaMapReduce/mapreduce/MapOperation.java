package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class MapOperation implements Callable<MapOperation.MapResult> {

    private static final String SEPARATORS = ";:/?˜\\.,><'[]{}()!@#$%ˆ&-_+=*\" \t\r\n";

    private final String filename;
    private final int offset;
    private final int fragmentSize;

    public MapOperation(String filename, int offset, int fragmentSize) {
        this.filename = filename;
        this.offset = offset;
        this.fragmentSize = fragmentSize;
    }

    @Override
    public MapResult call() {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filename))) {
            int remainingFragmentSize = fragmentSize;

            if (offset > 0) {
                bufferedReader.skip(offset - 1);
                char characterBeforeFragmentStart = (char) bufferedReader.read();
                int skippedCharacters = 0;
                while (!SEPARATORS.contains(String.valueOf(characterBeforeFragmentStart))) {
                    int characterRead = bufferedReader.read();
                    if (characterRead == -1) {
                        break;
                    }
                    characterBeforeFragmentStart = (char) characterRead;
                    ++skippedCharacters;
                }
                remainingFragmentSize -= skippedCharacters;
            }

            if (remainingFragmentSize > 0) {
                char[] characters = new char[remainingFragmentSize * 2];
                int charactersRead = bufferedReader.read(characters, 0, remainingFragmentSize);
                while (!SEPARATORS.contains(String.valueOf(characters[charactersRead - 1]))) {
                    int characterRead = bufferedReader.read();
                    if (characterRead == -1) {
                        break;
                    }
                    characters[charactersRead] = (char) characterRead;
                    ++charactersRead;
                }

                StringBuilder stringBuilder = new StringBuilder();
                for (int characterIndex = 0; characterIndex < charactersRead; ++characterIndex) {
                    stringBuilder.append(characters[characterIndex]);
                }

                String textFragment = stringBuilder.toString();
                StringTokenizer stringTokenizer = new StringTokenizer(textFragment, SEPARATORS);
                List<String> words = new ArrayList<>();
                while (stringTokenizer.hasMoreTokens()) {
                    words.add(stringTokenizer.nextToken());
                }

                int longestWordLength =
                    words
                        .stream()
                        .map(String::length)
                        .max(Integer::compareTo)
                        .orElse(0);

                List<String> longestWords =
                    words
                        .stream()
                        .filter(word -> word.length() == longestWordLength)
                        .collect(Collectors.toList());

                Map<Integer, Integer> wordSizeAndAppearanceCount = new HashMap<>();
                words
                    .forEach(word -> {
                        if (!wordSizeAndAppearanceCount.containsKey(word.length())) {
                            wordSizeAndAppearanceCount.put(word.length(), 1);
                        } else {
                            wordSizeAndAppearanceCount.put(word.length(), wordSizeAndAppearanceCount.get(word.length()) + 1);
                        }
                    });

                return new MapResult(filename, longestWords, wordSizeAndAppearanceCount);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new MapResult(filename, new ArrayList<>(), new HashMap<>());
    }

    public static class MapResult {

        private final String filename;
        private final List<String> longestWords;
        private final Map<Integer, Integer> wordSizeAndAppearanceCount;

        public MapResult(String filename, List<String> maxLengthWords, Map<Integer, Integer> wordSizeAndAppearanceCount) {
            this.filename = filename;
            this.longestWords = maxLengthWords;
            this.wordSizeAndAppearanceCount = wordSizeAndAppearanceCount;
        }

        public String getFilename() {
            return filename;
        }

        public List<String> getLongestWords() {
            return longestWords;
        }

        public Map<Integer, Integer> getWordSizeAndAppearanceCount() {
            return wordSizeAndAppearanceCount;
        }

    }

}
