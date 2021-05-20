package recovida.idas.rl.util;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import recovida.idas.rl.Main;

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
        LOGGER.warn(String.format(
                "Ignoring column %d_ because some items are missing.\n",
                number));
    }

    public void errorConfigFileDoesNotExist(String configFileName) {
        LOGGER.error(String.format("Configuration file \"%s\" does not exist.",
                configFileName));
    }

    public void infoUsingConfigFile(String configFileName) {
        LOGGER.info(String.format("Using configuration file \"%s\".",
                configFileName));
    }

    public void infoReadingA(String fileName) {
        LOGGER.info(String.format("Reading dataset A (\"%s\")...", fileName));
    }

    public void infoReadingAndIndexingB(String fileName) {
        LOGGER.info(String.format("Reading and indexing dataset B (\"%s\")...",
                fileName));
    }

    public void infoFinishedIndexingB(long n) {
        LOGGER.info("Finished reading and indexing dataset B (entries: " + n
                + ").");
    }

    public void infoReusingIndex() {
        LOGGER.info("Database B has already been indexed. Reusing index.");
    }

    public void infoOldIndexLacksColumns() {
        LOGGER.info("Database B has already been indexed, but the old index does not contain some of the required columns. Indexing it again.");
    }

    public void infoOldIndexIsCorrupt() {
        LOGGER.info("Indexing of database B has probably been interrupted in a previous execution. Indexing it again.");
    }

    public void infoFinishedReadingA(long n) {
        LOGGER.info("Finished reading dataset A (entries: " + n + ").");
    }

    public void infoMaxRowsA(long n) {
        LOGGER.info(String.format("Linking at most %d item(s).", n));
    }

    public void infoPerformingLinkage() {
        LOGGER.info("Performing linkage...");
    }

    public void infoCompleted(String resultPath) {
        LOGGER.info(String.format("Completed. The result was saved in \"%s\".",
                resultPath));
    }

    public void errorDatasetFileDoesNotExist(String fileName) {
        LOGGER.error(String.format("Could not find file: \"%s\".", fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName) {
        LOGGER.error(String.format("Could not read file: \"%s\".", fileName));
    }

    public void errorDatasetFileFormatIsUnsupported(String fileName) {
        LOGGER.error(String.format("Unsupported format: \"%s\".", fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName, String encoding) {
        LOGGER.error(String.format(
                "Could not read file \"%s\" using encoding \"%s\".", fileName,
                encoding));
    }

    public void errorOldIndexCannotBeDeleted(String dir) {
        LOGGER.error(String.format(
                "Could not delete old index. Please delete the directory \"%s\" and try again.",
                dir));
    }

    public void warnInvalidValueForType(String value, String type) {
        LOGGER.warn(String.format("Invalid value for type \"%s\":  \"%s\"", type, value));
    }

    public void warnCouldNotLinkRow() {
        LOGGER.warn("Could not link row.");
    }

    public void errorCannotSaveResult() {
        LOGGER.error("Error while writing file.");
    }

    public void infoSavingResult() {
        LOGGER.info("Saving linkage result...");
    }
}
