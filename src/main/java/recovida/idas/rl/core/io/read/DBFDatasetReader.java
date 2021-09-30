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

import recovida.idas.rl.core.io.AbstractDatasetRecord;

/**
 * Provides a mechanism to read a dataset from a DBF file.
 */
public class DBFDatasetReader implements DatasetReader {

    protected String fileName;

    protected String encoding;

    /**
     * Creates an instance.
     * 
     * @param fileName name of the DBF file containing the dataset
     * @param encoding the encoding of the file (see {@link Charset})
     */
    public DBFDatasetReader(String fileName, String encoding) {
        this.fileName = fileName;
        this.encoding = encoding;
    }

    @Override
    public Iterable<AbstractDatasetRecord> getDatasetRecordIterable() {
        try {
            return new Iterable<AbstractDatasetRecord>() {

                DBFReader dbf = new DBFReader(new FileInputStream(fileName),
                        Charset.forName(encoding));
                Map<String, Integer> keyToIndex = IntStream
                        .range(0, dbf.getFieldCount()).boxed().collect(
                                Collectors.toMap(i -> dbf.getField(i).getName(),
                                        Function.identity()));

                @Override
                public Iterator<AbstractDatasetRecord> iterator() {

                    return new Iterator<AbstractDatasetRecord>() {

                        Object[] nextItem = dbf.nextRecord();
                        long n = 1;

                        @Override
                        public boolean hasNext() {
                            return nextItem != null;
                        }

                        @Override
                        public AbstractDatasetRecord next() {
                            Object[] r = nextItem;
                            nextItem = dbf.nextRecord();
                            return AbstractDatasetRecord.fromKeyToIndexAndArray(n++,
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
