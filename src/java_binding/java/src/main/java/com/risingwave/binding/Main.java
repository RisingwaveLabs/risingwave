package com.risingwave.binding;

import java.util.Arrays;

/**
 * Hello world!
 */
public class Main {
    public static void main(String[] args) {
        try (Iterator iter = new Iterator()) {
            while (true) {
                try (Record record = iter.next()) {
                    if (record == null) {
                        break;
                    }
                    System.out.printf("key %s, id: %d, name: %s, is null: %s%n", Arrays.toString(record.getKey()), record.getLong(0), record.getString(1), record.isNull(2));
                }
            }
        }
    }
}
