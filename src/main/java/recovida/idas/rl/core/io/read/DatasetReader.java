package recovida.idas.rl.core.io.read;

import recovida.idas.rl.core.io.DatasetRecord;

public interface DatasetReader {

    Iterable<DatasetRecord> getDatasetRecordIterable();

}
