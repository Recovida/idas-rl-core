package recovida.idas.rl.core.io.read;

import java.io.FileInputStream;
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

import recovida.idas.rl.core.io.DatasetRecord;

public class CSVDatasetReader implements DatasetReader {

    protected String fileName;
    protected String encoding;
    protected char delimiter;
    private final CSVFormat format;

    public CSVDatasetReader(String fileName, char delimiter, String encoding) {
        this.fileName = fileName;
        this.encoding = encoding;
        this.delimiter = delimiter;
        format = CSVFormat.RFC4180.withFirstRecordAsHeader()
                .withDelimiter(delimiter).withQuote(null);
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
        } catch (IOException e) {
            return null;
        }
    }

    public static char guessCsvDelimiter(String fileName, String encoding) {
        String firstLine = null;
        try {
            firstLine = Files
                    .lines(Paths.get(fileName), Charset.forName(encoding))
                    .findFirst().get();
        } catch (UncheckedIOException | IOException e) {
            return '\0';
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
