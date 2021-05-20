package recovida.idas.rl.util;

import java.text.MessageFormat;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import recovida.idas.rl.Main;
import recovida.idas.rl.lang.MessageProvider;

public class StatusReporter {

    public final static Logger LOGGER = LogManager.getLogger(Main.class);

    protected static StatusReporter instance = null;

    public static StatusReporter get() {
        if (instance == null)
            instance = new StatusReporter();
        return instance;
    }

    public static void set(StatusReporter instance) { // can be used by subclasses
        if (instance != null)
            StatusReporter.instance = instance;
    }

    public void warnIgnoringColumn(int number) {
        LOGGER.warn(MessageFormat.format(
                MessageProvider.getMessage("config.ignoringnum"),
                number));
    }

    public void errorConfigFileDoesNotExist(String configFileName) {
        LOGGER.error(MessageFormat.format(MessageProvider.getMessage("config.doesnotexist"),
                configFileName));
    }

    public void infoUsingConfigFile(String configFileName) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("config.using"),
                configFileName));
    }

    public void infoReadingA(String fileName) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("dataset.a.reading"),
                fileName));
    }

    public void infoReadingAndIndexingB(String fileName) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("dataset.b.reading"),
                fileName));
    }

    public void infoFinishedIndexingB(long n) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("dataset.b.finishedreading"),
                n));
    }

    public void infoReusingIndex() {
        LOGGER.info(MessageProvider.getMessage("dataset.b.reusingindex"));
    }

    public void infoOldIndexLacksColumns() {
        LOGGER.info(MessageProvider.getMessage("dataset.b.incompleteindex"));
    }

    public void infoOldIndexIsCorrupt() {
        LOGGER.info(MessageProvider.getMessage("dataset.b.corruptindex"));
    }

    public void infoFinishedReadingA(long n) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("dataset.a.finishedreading"),
                n));
    }

    public void infoMaxRowsA(long n) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("linkage.maxrows"),
                n));
    }

    public void infoPerformingLinkage() {
        LOGGER.info(MessageProvider.getMessage("linkage.linking"));
    }

    public void infoCompleted(String resultPath) {
        LOGGER.info(MessageFormat.format(MessageProvider.getMessage("linkage.done"),
                resultPath));
    }

    public void errorDatasetFileDoesNotExist(String fileName) {
        LOGGER.error(MessageFormat.format(MessageProvider.getMessage("dataset.doesnotexist"),
                fileName));
    }

    public void errorDatasetFileFormatIsUnsupported(String fileName) {
        LOGGER.error(MessageFormat.format(MessageProvider.getMessage("dataset.unsupportedformat"),
                fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName, String encoding) {
        LOGGER.error(MessageFormat.format(MessageProvider.getMessage("dataset.couldnotread"),
                fileName, encoding));
    }

    public void errorOldIndexCannotBeDeleted(String dir) {
        LOGGER.error(MessageFormat.format(MessageProvider.getMessage("dataset.b.cannotdeleteoldindex"),
                dir));
    }

    public void warnInvalidValueForType(String value, String type) {
        LOGGER.warn(MessageFormat.format(MessageProvider.getMessage("dataset.invalidvalue"),
                value, type));
    }

    public void warnCouldNotLinkRow() {
        LOGGER.warn(MessageProvider.getMessage("linkage.cannotlink"));
    }

    public void errorCannotSaveResult() {
        LOGGER.error(MessageProvider.getMessage("linkage.cannotsave"));
    }

    public void infoSavingResult() {
        LOGGER.info(MessageProvider.getMessage("linkage.saving"));
    }

}
