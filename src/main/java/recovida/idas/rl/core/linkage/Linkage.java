package recovida.idas.rl.core.linkage;

import java.io.IOException;
import java.io.Serializable;

import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.record.RecordPairModel;
import recovida.idas.rl.core.search.Searching;

public class Linkage implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ConfigModel config;

    public Linkage(ConfigModel config) {
        this.config = config;
    }

    public RecordPairModel link(RecordModel record) {
        Searching searching;
        try {
            searching = new Searching(config);
        } catch (IOException e) {
            return null;
        }
        RecordPairModel candidatePair = searching
                .getCandidatePairFromRecord(record);
        if (candidatePair == null) {
            return null;
        }
        if (candidatePair.getScore() >= config.getMinimumScore())
            return candidatePair;
        return null;
    }

}
