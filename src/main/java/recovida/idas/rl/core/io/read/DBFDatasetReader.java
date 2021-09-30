package recovida.idas.rl.core.io.read;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFReader;

import recovida.idas.rl.core.io.DatasetRecord;

/**
 * Provides a mechanism to read a dataset from a DBF file.
 */
public class DBFDatasetReader implements DatasetReader {

    protected String fileName;
    
    protected String encoding;

    public DBFDatasetReader(String fileName, String encoding) {
        this.fileName = fileName;
        this.encoding = encoding;
    }

    @Override
    public Iterable<DatasetRecord> getDatasetRecordIterable() {
        try {
            return new Iterable<DatasetRecord>() {

                DBFReader dbf = new DBFReader(new FileInputStream(fileName),
                        Charset.forName(encoding));
                Map<String, Integer> keyToIndex = IntStream
                        .range(0, dbf.getFieldCount()).boxed().collect(
                                Collectors.toMap(i -> dbf.getField(i).getName(),
                                        Function.identity()));

                @Override
                public Iterator<DatasetRecord> iterator() {

                    return new Iterator<DatasetRecord>() {

                        Object[] nextItem = dbf.nextRecord();
                        long n = 1;

                        @Override
                        public boolean hasNext() {
                            return nextItem != null;
                        }

                        @Override
                        public DatasetRecord next() {
                            Object[] r = nextItem;
                            nextItem = dbf.nextRecord();
                            return DatasetRecord.fromKeyToIndexAndArray(n++,
                                    keyToIndex, r);
                        }
                    };

                }
            };
        } catch (IOException | DBFException e) {
            return null;
        }
    }

}
