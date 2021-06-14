package recovida.idas.rl.core.util;

import java.io.PrintStream;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.function.Supplier;

import recovida.idas.rl.core.lang.MessageProvider;

public class StatusReporter {

    public enum LoggingLevel {

        NONE(0), FATAL(1), ERROR(2), WARN(3), INFO(4), DEBUG(5),
        ALL(Integer.MAX_VALUE);

        int level;

        LoggingLevel(int level) {
            this.level = level;
        }

        public boolean logs(LoggingLevel that) {
            return level >= that.level;
        }
    }

    public interface StatusLogger {

        void warn(Supplier<String> message);

        void info(Supplier<String> message);

        void infoWithoutLineBreak(Supplier<String> message);

        void error(Supplier<String> message);

        void debug(Supplier<String> message);

    }

    public static class ConsoleLogger implements StatusLogger {

        @Override
        public void warn(Supplier<String> message) {
            log(System.err, "WARN", message.get());
        }

        @Override
        public void info(Supplier<String> message) {
            log(System.out, "INFO", message.get());
        }

        @Override
        public void infoWithoutLineBreak(Supplier<String> message) {
            log(System.out, "INFO", message.get(), false);
        }

        @Override
        public void error(Supplier<String> message) {
            log(System.err, "ERROR", message.get());
        }

        @Override
        public void debug(Supplier<String> message) {
            log(System.err, "DEBUG", message.get());
        }

        public void log(PrintStream stream, String type, String message) {
            log(stream, type, message, true);
        }

        public void log(PrintStream stream, String type, String message,
                boolean lineBreak) {
            stream.format("[%8s] %5s: %s" + (lineBreak ? "%n" : ""),
                    LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS).format(
                            DateTimeFormatter.ISO_LOCAL_TIME),
                    type, message);
        }

    }

    public static LoggingLevel currentLevel = LoggingLevel.INFO;
    protected StatusLogger logger = new ConsoleLogger();

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
            logger.warn(() -> MessageFormat.format(
                    MessageProvider.getMessage("config.ignoringnum"), number));
    }

    public void warnErrorQuery(String query, String trace) {
        if (currentLevel.logs(LoggingLevel.WARN))
            logger.warn(() -> MessageFormat.format(
                    MessageProvider.getMessage("linkage.queryerror"), query, trace));
    }

    public void errorConfigFileDoesNotExist(String configFileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("config.doesnotexist"),
                    configFileName));
    }

    public void errorSameSeparator() {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() ->
                    MessageProvider.getMessage("config.sameseparator"));
    }

    public void errorConfigFileHasInvalidValue(String value, String field) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("config.invalidvalue"), value,
                    field));
    }

    public void infoUsingConfigFile(String configFileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("config.using"),
                    configFileName));
    }

    public void infoReadingA(String fileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.reading"), fileName));
    }

    public void infoReadingAndIndexingB(String fileName) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.reading"), fileName));
    }

    public void errorMissingColumnInDatasetA(String column) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.missingcolumn"),
                    column));
    }

    public void errorMissingColumnInDatasetB(String column) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.missingcolumn"),
                    column));
    }

    public void infoAvailableColumnsInDatasetA(String column) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.columnlist"),
                    column));
    }

    public void infoAvailableColumnsInDatasetB(String column) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.columnlist"),
                    column));
    }

    public void infoFinishedIndexingB(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.finishedreading"),
                    n));
    }

    public void infoReusingIndex(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.reusingindex"), n));
    }

    public void infoOldIndexLacksColumns() {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageProvider
                    .getMessage("dataset.b.incompleteindex"));
    }

    public void infoOldIndexHasDifferentCleaningPattern() {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageProvider
                    .getMessage("dataset.b.differentcleaningpatternonindex"));
    }

    public void infoOldIndexIsCorrupt() {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(
                    () -> MessageProvider.getMessage("dataset.b.corruptindex"));
    }

    public void infoFinishedReadingA(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.a.finishedreading"),
                    n));
    }

    public void infoMaxRowsA(long n) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat
                    .format(MessageProvider.getMessage("linkage.maxrows"), n));
    }

    public void infoPerformingLinkage(int nThreads) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("linkage.linking"), nThreads));
    }

    public void infoCompleted(String resultPath) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.info(() -> MessageFormat.format(
                    MessageProvider.getMessage("linkage.done"), resultPath));
    }

    public void errorDatasetFileDoesNotExist(String fileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.doesnotexist"),
                    fileName));
    }

    public void errorDatasetFileFormatIsUnsupported(String fileName) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.unsupportedformat"),
                    fileName));
    }

    public void errorDatasetFileCannotBeRead(String fileName, String encoding) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.couldnotread"),
                    fileName, encoding));
    }

    public void errorOldIndexCannotBeDeleted(String dir) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(MessageProvider
                    .getMessage("dataset.b.cannotdeleteoldindex"), dir));
    }

    public void warnInvalidValueForType(String value, String type) {
        if (currentLevel.logs(LoggingLevel.WARN))
            logger.warn(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.invalidvalue"), value,
                    type));
    }

    public void infoLinkageProgress(float done) {
        if (currentLevel.logs(LoggingLevel.INFO))
            logger.infoWithoutLineBreak(
                    () -> MessageFormat.format(
                            MessageProvider.getMessage(
                                    "linkage.progresspercentage") + '\r',
                            done));
    }

    public void warnCouldNotLinkRow() {
        if (currentLevel.logs(LoggingLevel.WARN))
            logger.warn(() -> MessageProvider.getMessage("linkage.cannotlink"));
    }

    public void errorCannotSaveResult() {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(
                    () -> MessageProvider.getMessage("linkage.cannotsave"));
    }

    public void errorUnexpectedError(String message) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("linkage.unexpectederror"),
                    message));
    }

    public void errorMissingFieldInConfigFile(String field) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("config.missingfield"), field));
    }

    public void errorCannotIndex(String dir) {
        if (currentLevel.logs(LoggingLevel.ERROR))
            logger.error(() -> MessageFormat.format(
                    MessageProvider.getMessage("dataset.b.cannotindex"), dir));
    }

    public void warnInterrupted() {
        if (currentLevel.logs(LoggingLevel.WARN))
            logger.warn(() -> MessageProvider.getMessage("interrupted"));
    }

    public StatusLogger getLogger() {
        return logger;
    }

    public void setLogger(StatusLogger logger) {
        this.logger = logger;
    }


}
