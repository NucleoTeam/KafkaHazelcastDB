package com.nucleocore.db.database.utils;

import com.google.common.collect.Lists;
import java.util.*;
import java.util.stream.Collectors;

public class PositionReduce {
    public class CharacterIndex {
        char character;
        List<Integer> entries = new ArrayList<>();

        public CharacterIndex(char character) {
            this.character = character;
        }

        public synchronized void add(int index) {
            entries.add(index);
            //Collections.sort(entries);
        }
    }

    public class RootIndex {
        public RootIndex() {
        }

        List<CharacterIndex> characters = Lists.newArrayList();

        void add(char character, int index) {
            Optional<CharacterIndex> charIndex;
            synchronized (characters) {
                if ((charIndex = characters.stream().filter((a) -> a.character == character).findFirst()).isPresent()) {
                    charIndex.get().add(index);
                } else {
                    CharacterIndex ci = new CharacterIndex(character);
                    characters.add(ci);
                    ci.add(index);
                }
            }
        }

        CharacterIndex get(char character) {
            List<CharacterIndex> charIndex;
            if ((charIndex = characters.stream().filter((a) -> a.character == character).collect(Collectors.toList())) != null) {
                return charIndex.get(0);
            }
            return null;
        }
    }

    public RootIndex[] positionIndex;

    int indexLength;

    public PositionReduce(int indexLength) {
        this.indexLength = indexLength;
        positionIndex = new RootIndex[indexLength];
        for (int k = 0; k < indexLength; k++)
            positionIndex[k] = new RootIndex();
    }

    public void add(String string, int index) {
        char[] chars = string.toCharArray();
        int len = chars.length;
        for (int x = 0; x < len; x++) {
            positionIndex[x].add(chars[x], index);
        }
    }

    public List<Integer> find(String string) {

        char[] chars = string.toCharArray();
        int len = chars.length;

        if (len <= 2)
            return null;

        List<Integer> finalList = null;
        CharacterIndex current;

        for (int x = 0; x < len; x++) {

            current = positionIndex[x].get(chars[x]);
            synchronized (current) {
                if (current == null)
                    return null;

                List<Integer> indexList = new ArrayList<>();
                int pos = 0;

                if (finalList != null) {
                    int curLen = current.entries.size();
                    int prevLen = finalList.size();
                    int k = 0;
                    int c = 0;
                    while (k < prevLen) {
                        if (k >= prevLen || c >= curLen)
                            break;
                        int kval = finalList.get(k);
                        int cval = current.entries.get(c);
                        if (kval < cval) {
                            k++;
                        } else if (cval < kval) {
                            c++;
                        } else {
                            indexList.add(cval);
                            pos++;
                            k++;
                            c++;
                        }
                    }
                    finalList = indexList;

                } else
                    finalList = current.entries;
            }
        }
        return finalList;
    }
}
