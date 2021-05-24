package recovida.idas.rl.io.read;

import recovida.idas.rl.io.DatasetRecord;

public interface DatasetReader {


    Iterable<DatasetRecord> getDatasetRecordIterable();

}
