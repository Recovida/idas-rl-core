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

import recovida.idas.rl.core.io.DatasetRecord;

public class CSVDatasetReader implements DatasetReader {

    protected String fileName;
    protected String encoding;
    protected char delimiter;

    public CSVDatasetReader(String fileName, String encoding) {
        this.fileName = fileName;
        this.encoding = encoding;
    }

    @Override
    public Iterable<DatasetRecord> getDatasetRecordIterable() {
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
            in = new InputStreamReader(isWithoutBOM, Charset.forName(encoding).newDecoder());
            IterableResult<Record, ParsingContext> records = parser.iterateRecords(in);
            settings.detectFormatAutomatically();
            return DatasetRecord.fromCSVRecordIterable(records);
        } catch (IOException e) {
            return null;
        }
    }


}
