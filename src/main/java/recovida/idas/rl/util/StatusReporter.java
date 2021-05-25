package recovida.idas.rl.util;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import recovida.idas.rl.lang.MessageProvider;

public class StatusReporter {

    public enum LoggingLevel {

        NONE(0), FATAL(1), ERROR(2), WARN(3), INFO(4), DEBUG(5),
        ALL(Integer.MAX_VALUE);

        int level;

        LoggingLevel(int level) {
            this.level = level;
        }

        public boolean logs(LoggingLevel that) {
            return this.level >= that.level;
        }
    }

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

        public static void log(PrintStream stream, String type, String message,
                boolean lineBreak) {
            stream.format("[%8s] %5s: %s" + (lineBreak ? "%n" : ""),
                    LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).format(
                            DateTimeFormatter.ISO_LOCAL_TIME),
                    type, message);
        }

    }

    public static LoggingLevel currentLevel = LoggingLevel.INFO;

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
        if (currentLevel.logs(LoggingLevel.WARN))
            ConsoleLogger.warn(MessageFormat.format(
                    MessageProvider.getMessage("config.ignoringnum"), number));
    }

    public void errorConfigFileDoesNotExist(String configFileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("config.doesnotexist"),
                    configFileName));
    }

    public void infoUsingConfigFile(String configFileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("config.using"),
                    configFileName));
    }

    public void infoReadingA(String fileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.reading"), fileName));
    }

    public void infoReadingAndIndexingB(String fileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.reading"), fileName));
    }

    public void errorMissingColumnInDatasetA(String column) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.missingcolumn"),
                    column));
    }

    public void errorMissingColumnInDatasetB(String column) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.missingcolumn"),
                    column));
    }

    public void infoAvailableColumnsInDatasetA(String column) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.columnlist"),
                    column));
    }

    public void infoAvailableColumnsInDatasetB(String column) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.columnlist"),
                    column));
    }

    public void infoFinishedIndexingB(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.finishedreading"),
                    n));
    }

    public void infoReusingIndex() {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger
            .info(MessageProvider.getMessage("dataset.b.reusingindex"));
    }

    public void infoOldIndexLacksColumns() {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(
                    MessageProvider.getMessage("dataset.b.incompleteindex"));
    }

    public void infoOldIndexIsCorrupt() {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger
            .info(MessageProvider.getMessage("dataset.b.corruptindex"));
    }

    public void infoFinishedReadingA(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.finishedreading"),
                    n));
    }

    public void infoMaxRowsA(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat
                    .format(MessageProvider.getMessage("linkage.maxrows"), n));
    }

    public void infoPerformingLinkage(int nThreads) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("linkage.linking"), nThreads));
    }

    public void infoCompleted(String resultPath) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger.info(MessageFormat.format(
                    MessageProvider.getMessage("linkage.done"), resultPath));
    }

    public void errorDatasetFileDoesNotExist(String fileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.doesnotexist"),
                    fileName));
    }

    public void errorDatasetFileFormatIsUnsupported(String fileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.unsupportedformat"),
                    fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName, String encoding) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("dataset.couldnotread"),
                    fileName, encoding));
    }

    public void errorOldIndexCannotBeDeleted(String dir) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(MessageProvider
                    .getMessage("dataset.b.cannotdeleteoldindex"), dir));
    }

    public void warnInvalidValueForType(String value, String type) {
        if (currentLevel.logs(LoggingLevel.WARN))
            ConsoleLogger.warn(MessageFormat.format(
                    MessageProvider.getMessage("dataset.invalidvalue"), value,
                    type));
    }

    public void infoLinkageProgress(float done) {
        if (currentLevel.logs(LoggingLevel.INFO))
            ConsoleLogger
            .infoWithoutLineBreak(MessageFormat.format(
                    MessageProvider.getMessage(
                            "linkage.progresspercentage") + '\r',
                    done));
    }

    public void warnCouldNotLinkRow() {
        if (currentLevel.logs(LoggingLevel.WARN))
            ConsoleLogger.warn(
                    MessageProvider.getMessage("linkage.cannotlink"));
    }

    public void errorCannotSaveResult() {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger
            .error(MessageProvider.getMessage("linkage.cannotsave"));
    }

    public void errorMissingFieldInConfigFile(String field) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            ConsoleLogger.error(MessageFormat.format(
                    MessageProvider.getMessage("config.missingfield"), field));
    }

}
