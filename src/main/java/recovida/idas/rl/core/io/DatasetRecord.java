package recovida.idas.rl.core.io;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVRecord;

public abstract class DatasetRecord {

    public abstract String get(String key);

    public abstract long getNumber();

    public abstract Collection<String> getKeySet();

    public static DatasetRecord fromKeyToIndexAndArray(long number,
            Map<String, Integer> map, Object[] arr) {
        return new DatasetRecord() {

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

    public static DatasetRecord fromCSVRecord(long number,
            CSVRecord csvRecord) {
        return new DatasetRecord() {

            @Override
            public String get(String key) {
                try {
                    return csvRecord.get(key);
                } catch (IllegalArgumentException e) {
                    if (!csvRecord.isConsistent()) {
                        // Apache Commons CSV parser does not support CSV files
                        // that end a line prematurely when the last values are
                        // null.
                        return "";
                    }
                    throw e;
                }
            }

            @Override
            public long getNumber() {
                return number;
            }

            @Override
            public Collection<String> getKeySet() {
                return Collections.unmodifiableSet(csvRecord.toMap().keySet());
            }
        };
    }

    public static Iterable<DatasetRecord> fromCSVRecordIterable(
            Iterable<CSVRecord> csvIterable) {
        return () -> {
            Iterator<CSVRecord> oldIt = csvIterable.iterator();
            return new Iterator<DatasetRecord>() {

                long n = 1;

                @Override
                public boolean hasNext() {
                    return oldIt.hasNext();
                }

                @Override
                public DatasetRecord next() {
                    return DatasetRecord.fromCSVRecord(n++, oldIt.next());
                }
            };
        };
    }

}
