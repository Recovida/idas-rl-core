package recovida.idas.rl.core.io.read;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;

import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import recovida.idas.rl.core.io.AbstractDatasetRecord;

/**
 * Provides a mechanism to read a dataset from a CSV file.
 */
public class CSVDatasetReader implements DatasetReader {

    protected String fileName;

    protected String encoding;

    protected char delimiter;

    protected boolean lenient;

    /**
     * Creates an instance.
     * 
     * @param fileName name of the CSV file containing the dataset
     * @param encoding the encoding of the file (see {@link Charset})
     * @param lenient  whether unparsable bytes should be just ignored rather
     *                 than resulting in an exception
     */
    public CSVDatasetReader(String fileName, String encoding, boolean lenient) {
        this.fileName = fileName;
        this.encoding = encoding;
        this.lenient = lenient;
    }

    @Override
    public Iterable<AbstractDatasetRecord> getDatasetRecordIterable() {
        Reader in = null;
        CsvParserSettings settings = new CsvParserSettings();
        settings.setEmptyValue("");
        settings.setHeaderExtractionEnabled(true);
        CsvParser parser = new CsvParser(settings);

        try {
            FileInputStream fis = new FileInputStream(fileName);
            InputStream isWithoutBOM = new BOMInputStream(fis,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE,
                    ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE);
            in = lenient
                    ? new InputStreamReader(isWithoutBOM,
                            Charset.forName(encoding))
                    : new InputStreamReader(isWithoutBOM,
                            Charset.forName(encoding).newDecoder());
            IterableResult<Record, ParsingContext> records = parser
                    .iterateRecords(in);
            settings.detectFormatAutomatically();
            return AbstractDatasetRecord.fromCSVRecordIterable(records);
        } catch (IOException e) {
            return null;
        }
    }

}
