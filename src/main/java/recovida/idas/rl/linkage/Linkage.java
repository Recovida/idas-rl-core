package recovida.idas.rl.linkage;

import java.io.Serializable;

import org.apache.log4j.Logger;

import recovida.idas.rl.config.ConfigModel;
import recovida.idas.rl.record.RecordModel;
import recovida.idas.rl.record.RecordPairModel;
import recovida.idas.rl.search.Searching;
import recovida.idas.rl.util.StatusReporter;

public class Linkage implements Serializable {
    private static final long serialVersionUID = 1L;
    private ConfigModel config;

    public Linkage(ConfigModel config) {
        this.config = config;
    }

    public String linkSpark(RecordModel record) {
        Searching searching = new Searching(this.config);
        LinkageUtils linkageUtils = new LinkageUtils();
        RecordPairModel candidatePair = searching.getCandidatePairFromRecord(record);
        if (candidatePair != null) {
            if (candidatePair.getScore() >= config.getMinimumScore())
                return linkageUtils.fromRecordPairToCsv(candidatePair);
            else
                return "";
        } else {
            StatusReporter.get().warnCouldNotLinkRow();
            Logger.getLogger(getClass()).debug("This is the row that could not be linked: " + record.getColumnRecordModels());
            return "";
        }
    }

}
