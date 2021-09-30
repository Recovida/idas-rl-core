package recovida.idas.rl.core.io.read;

import recovida.idas.rl.core.io.AbstractDatasetRecord;

/**
 * Provides a mechanism to read a dataset from a file.
 */
public interface DatasetReader {

    /**
     * Returns the sequence of records read from the file.
     * 
     * @return an iterable of the records that are being read
     */
    Iterable<AbstractDatasetRecord> getDatasetRecordIterable();

}
