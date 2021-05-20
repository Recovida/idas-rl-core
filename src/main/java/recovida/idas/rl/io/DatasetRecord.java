package recovida.idas.rl.io;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.csv.CSVRecord;

public abstract class DatasetRecord {

    public abstract String get(String key);

    public static DatasetRecord fromKeyToIndexAndArray(Map<String, Integer> map, Object[] arr) {
        return new DatasetRecord() {

            @Override
            public String get(String key) {
                int i = map.get(key);
                if (arr == null || i < 0 || i >= arr.length)
                    return "";
                return Optional.ofNullable(arr[i]).orElse("").toString();
            }
        };
    }

    public static DatasetRecord fromCSVRecord(CSVRecord csvRecord) {
        return new DatasetRecord() {

            @Override
            public String get(String key) {
                return csvRecord.get(key);
            }
        };
    }

    public static Iterable<DatasetRecord> fromCSVRecordIterable(Iterable<CSVRecord> csvIterable) {
        return new Iterable<DatasetRecord>() {

            @Override
            public Iterator<DatasetRecord> iterator() {
                Iterator<CSVRecord> oldIt = csvIterable.iterator();
                return new Iterator<DatasetRecord>() {

                    @Override
                    public boolean hasNext() {
                        return oldIt.hasNext();
                    }

                    @Override
                    public DatasetRecord next() {
                        return DatasetRecord.fromCSVRecord(oldIt.next());
                    }
                };
            }
        };
    }



}
