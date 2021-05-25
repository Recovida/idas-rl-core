package recovida.idas.rl.io.read;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;

import recovida.idas.rl.io.DatasetRecord;
import recovida.idas.rl.util.StatusReporter;

public class CSVDatasetReader implements DatasetReader {

    protected String fileName;
    protected String encoding;
    protected char delimiter;
    private CSVFormat format;

    public CSVDatasetReader(String fileName, char delimiter, String encoding) {
        this.fileName = fileName;
        this.encoding = encoding;
        this.delimiter = delimiter;
        this.format = CSVFormat.RFC4180.withFirstRecordAsHeader()
                .withDelimiter(delimiter);
    }

    @Override
    public Iterable<DatasetRecord> getDatasetRecordIterable() {
        Reader in = null;
        try {
            FileInputStream fis = new FileInputStream(fileName);
            InputStream isWithoutBOM = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                    ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE);
            in = new InputStreamReader(isWithoutBOM, Charset.forName(encoding));
            Iterable<CSVRecord> records = format.parse(in);

            return DatasetRecord.fromCSVRecordIterable(records);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static char guessCsvDelimiter(String fileName, String encoding)
            throws IOException {
        String firstLine = null;
        try {
            firstLine = Files
                    .lines(Paths.get(fileName), Charset.forName(encoding))
                    .findFirst().get();
        } catch (UncheckedIOException e) {
            StatusReporter.get().errorDatasetFileCannotBeRead(fileName,
                    encoding);
            System.exit(1);
        }
        char[] delimiters = { ',', ';', '|', '\t' };
        char delimiter = '\0';
        long occurrences = -1;
        for (char sep : delimiters) {
            long n = firstLine.chars().filter(ch -> ch == sep).count();
            if (n > occurrences) {
                delimiter = sep;
                occurrences = n;
            }
        }
        return delimiter;
    }

}
