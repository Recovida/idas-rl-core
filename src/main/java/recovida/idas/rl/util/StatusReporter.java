package recovida.idas.rl.util;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import recovida.idas.rl.lang.MessageProvider;

public class StatusReporter {

    public static class ConsoleLogger {

        public static void warn(String message) {
            log(System.err, "WARN", message);
        }

        public static void info(String message) {
            log(System.out, "INFO", message);
        }

        public static void infoWithoutLineBreak(String message) {
            log(System.out, "INFO", message, false);
        }

        public static void error(String message) {
            log(System.err, "ERROR", message);
        }

        public static void debug(String message) {
            log(System.err, "DEBUG", message);
        }

        public static void log(PrintStream stream, String type,
                String message) {
            log(stream, type, message, true);
        }

        public static void log(PrintStream stream, String type,
                String message, boolean lineBreak) {
            stream.format("[%8s] %5s: %s" + (lineBreak ? "%n" : ""),
                    LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).format(
                            DateTimeFormatter.ISO_LOCAL_TIME),
                    type, message);
        }


    }

    protected static StatusReporter instance = null;

    public static StatusReporter get() {
        if (instance == null)
            instance = new StatusReporter();
        return instance;
    }

    public static void set(StatusReporter instance) { // can be used by
        // subclasses
        if (instance != null)
            StatusReporter.instance = instance;
    }

    public void warnIgnoringColumn(int number) {
        ConsoleLogger.warn(MessageFormat.format(
                MessageProvider.getMessage("config.ignoringnum"), number));
    }

    public void errorConfigFileDoesNotExist(String configFileName) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("config.doesnotexist"),
                configFileName));
    }

    public void infoUsingConfigFile(String configFileName) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("config.using"), configFileName));
    }

    public void infoReadingA(String fileName) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("dataset.a.reading"), fileName));
    }

    public void infoReadingAndIndexingB(String fileName) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("dataset.b.reading"), fileName));
    }

    public void errorMissingColumnInDatasetA(String column) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.a.missingcolumn"), column));
    }

    public void errorMissingColumnInDatasetB(String column) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.b.missingcolumn"), column));
    }

    public void infoAvailableColumnsInDatasetA(String column) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.a.columnlist"), column));
    }

    public void infoAvailableColumnsInDatasetB(String column) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.b.columnlist"), column));
    }

    public void infoFinishedIndexingB(long n) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("dataset.b.finishedreading"), n));
    }

    public void infoReusingIndex() {
        ConsoleLogger
        .info(MessageProvider.getMessage("dataset.b.reusingindex"));
    }

    public void infoOldIndexLacksColumns() {
        ConsoleLogger
        .info(MessageProvider.getMessage("dataset.b.incompleteindex"));
    }

    public void infoOldIndexIsCorrupt() {
        ConsoleLogger
        .info(MessageProvider.getMessage("dataset.b.corruptindex"));
    }

    public void infoFinishedReadingA(long n) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("dataset.a.finishedreading"), n));
    }

    public void infoMaxRowsA(long n) {
        ConsoleLogger.info(MessageFormat
                .format(MessageProvider.getMessage("linkage.maxrows"), n));
    }

    public void infoPerformingLinkage(int nThreads) {
        ConsoleLogger.info(MessageFormat
                .format(MessageProvider.getMessage("linkage.linking"), nThreads));
    }

    public void infoCompleted(String resultPath) {
        ConsoleLogger.info(MessageFormat.format(
                MessageProvider.getMessage("linkage.done"), resultPath));
    }

    public void errorDatasetFileDoesNotExist(String fileName) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.doesnotexist"), fileName));
    }

    public void errorDatasetFileFormatIsUnsupported(String fileName) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.unsupportedformat"),
                fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName, String encoding) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.couldnotread"), fileName,
                encoding));
    }

    public void errorOldIndexCannotBeDeleted(String dir) {
        ConsoleLogger.error(MessageFormat.format(
                MessageProvider.getMessage("dataset.b.cannotdeleteoldindex"),
                dir));
    }

    public void warnInvalidValueForType(String value, String type) {
        ConsoleLogger.warn(MessageFormat.format(
                MessageProvider.getMessage("dataset.invalidvalue"), value,
                type));
    }

    public void infoLinkageProgress(float done) {
        ConsoleLogger.infoWithoutLineBreak(MessageFormat.format(
                MessageProvider.getMessage("linkage.progresspercentage") + '\r', done));
    }

    public void warnCouldNotLinkRow() {
        ConsoleLogger.warn(MessageProvider.getMessage("linkage.cannotlink"));
    }

    public void errorCannotSaveResult() {
        ConsoleLogger.error(MessageProvider.getMessage("linkage.cannotsave"));
    }

}
