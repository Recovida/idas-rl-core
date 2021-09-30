package recovida.idas.rl.core.io;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.record.Record;

/**
 * Represents a dataset record.
 */
public abstract class AbstractDatasetRecord {

    /**
     * Returns the value of the given column in this record.
     * 
     * @param key column name
     * @return value in the column
     */
    public abstract String get(String key);

    public abstract long getNumber();

    public abstract Collection<String> getKeySet();

    /**
     * Returns a dataset record having a given number, where values are provided
     * in an array and a map has the correspondence between column names and
     * array indices.
     * 
     * @param number number of the record
     * @param map    correspondence between column names and positions
     * @param arr    array with row data
     * @return a dataset record with the given data
     */
    public static AbstractDatasetRecord fromKeyToIndexAndArray(long number,
            Map<String, Integer> map, Object[] arr) {
        return new AbstractDatasetRecord() {

            @Override
            public String get(String key) {
                int i = map.get(key);
                if (arr == null || i < 0 || i >= arr.length)
                    return "";
                return Optional.ofNullable(arr[i]).orElse("").toString();
            }

            @Override
            public long getNumber() {
                return number;
            }

            @Override
            public Collection<String> getKeySet() {
                return Collections.unmodifiableSet(map.keySet());
            }
        };
    }

    /**
     * Returns a dataset record from a CSV record.
     * 
     * @param number    number of the record
     * @param csvRecord the CSV record
     * @return a dataset record containing the data from the CSV record
     */
    public static AbstractDatasetRecord fromCSVRecord(long number,
            Record csvRecord) {
        Map<String, String> contents = csvRecord.toFieldMap();
        List<String> keys = Collections.unmodifiableList(
                Arrays.asList(csvRecord.getMetaData().headers()));
        return new AbstractDatasetRecord() {

            @Override
            public String get(String key) {
                return Optional.ofNullable(contents.getOrDefault(key, null))
                        .orElse("");
            }

            @Override
            public long getNumber() {
                return number;
            }

            @Override
            public Collection<String> getKeySet() {
                return keys;
            }
        };
    }

    /**
     * Returns a sequence of dataset records from a sequence of CSV records.
     * 
     * @param csvIterable a sequence of CSV records
     * @return a sequence of dataset records containing the data from the CSV
     *         records
     */
    public static Iterable<AbstractDatasetRecord> fromCSVRecordIterable(
            IterableResult<Record, ParsingContext> csvIterable) {
        return () -> {
            Iterator<Record> oldIt = csvIterable.iterator();
            return new Iterator<AbstractDatasetRecord>() {

                long n = 1;

                @Override
                public boolean hasNext() {
                    return oldIt.hasNext();
                }

                @Override
                public AbstractDatasetRecord next() {
                    Record r = oldIt.next();
                    return AbstractDatasetRecord.fromCSVRecord(n++, r);
                }
            };
        };
    }

}
