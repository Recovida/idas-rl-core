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

    public static DatasetRecord fromCSVRecord(long number, Record csvRecord) {
        Map<String, String> contents = csvRecord.toFieldMap();
        List<String> keys = Collections.unmodifiableList(
                Arrays.asList(csvRecord.getMetaData().headers()));
        return new DatasetRecord() {

            @Override
            public String get(String key) {
                return Optional.ofNullable(contents.getOrDefault(key, null)).orElse("");
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

    public static Iterable<DatasetRecord> fromCSVRecordIterable(
            IterableResult<Record, ParsingContext> csvIterable) {
        return () -> {
            Iterator<Record> oldIt = csvIterable.iterator();
            return new Iterator<DatasetRecord>() {

                long n = 1;

                @Override
                public boolean hasNext() {
                    return oldIt.hasNext();
                }

                @Override
                public DatasetRecord next() {
                    Record r = oldIt.next();
                    return DatasetRecord.fromCSVRecord(n++, r);
                }
            };
        };
    }

}
