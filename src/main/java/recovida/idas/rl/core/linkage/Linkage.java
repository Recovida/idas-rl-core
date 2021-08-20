package recovida.idas.rl.core.linkage;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.record.RecordPairModel;
import recovida.idas.rl.core.search.Searching;

public class Linkage implements Serializable, Closeable {
    private static final long serialVersionUID = 1L;
    private final ConfigModel config;
    private Searching searching;

    public Linkage(ConfigModel config) {
        this.config = config;
        try {
            searching = new Searching(config);
        } catch (IOException e) {
            searching = null;
        }
    }

    public RecordPairModel link(RecordModel record) {
        if (searching == null || record == null)
            return null;
        RecordPairModel candidatePair = searching
                .getCandidatePairFromRecord(record);
        if (candidatePair == null) {
            return null;
        }
        if (candidatePair.getScore() >= config.getMinimumScore())
            return candidatePair;
        return null;
    }

    @Override
    public void close() {
        if (searching != null) {
            searching.close();
        }
    }

    @Override
    public void finalize() {
        close();
    }

}
