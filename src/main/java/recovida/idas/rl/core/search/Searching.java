package recovida.idas.rl.core.search;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import recovida.idas.rl.core.config.ColumnConfigModel;
import recovida.idas.rl.core.config.ConfigModel;
import recovida.idas.rl.core.record.ColumnRecordModel;
import recovida.idas.rl.core.record.RecordComparator;
import recovida.idas.rl.core.record.RecordModel;
import recovida.idas.rl.core.record.RecordPairModel;
import recovida.idas.rl.core.util.Permutation;
import recovida.idas.rl.core.util.StatusReporter;

public class Searching {
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private final Directory index;
    private IndexSearcher searcher;
    private QueryParser queryParser;
    private final IndexReader reader;
    private TopScoreDocCollector collector;
    private final ConfigModel config;
    private final SearchingUtils seachingUtils;
    private final Permutation permutation;

    public Searching(ConfigModel config) throws IOException {
        this.config = config;
        seachingUtils = new SearchingUtils();
        permutation = new Permutation();

        index = FSDirectory.open(Paths.get(config.getDbIndex()));
        reader = DirectoryReader.open(index);
    }

    public RecordPairModel getCandidatePairFromRecord(RecordModel record) {
        int HITS = 100;
        String strBusca;
        ArrayList<RecordModel> tmpCandidates;
        final ArrayList<ColumnRecordModel> filteredColumns;
        ArrayList<ColumnRecordModel> tmpColumns;
        RecordComparator recordComparator = new RecordComparator(config);
        RecordPairModel tmpCandidate;

        filteredColumns = seachingUtils
                .filterUnusedColumns(record.getColumnRecordModels());
        // filter unused columns
        String recordId = record.getColumnRecordModels().get(0).getValue();

        // FASE 1
        strBusca = seachingUtils.getStrQueryExact(filteredColumns);
        // DO THE QUERY
        tmpCandidates = searchCandidateRecordsFromStrQuery(strBusca, HITS,
                recordId);
        // IF ANY RESULT WAS FOUND
        if (tmpCandidates.isEmpty() == false) {
            tmpCandidate = recordComparator.findBestCandidatePair(record,
                    tmpCandidates);

            if (tmpCandidate != null && tmpCandidate.getScore() >= 0.95) {
                return tmpCandidate;
            }
        }

        // FASE 2
        tmpCandidates = new ArrayList<>();
        // tmpCandidates.add(candidate);
        // GENERATE POSSIBLE COMBINATIONS
        ArrayList<ArrayList<Integer>> combinacoes = permutation
                .combine(filteredColumns.size(), filteredColumns.size() - 1);
        // DO EACH SEARCH BASED ON COMBINATIONS GENERATED
        for (ArrayList<Integer> combinacao : combinacoes) {
            // seleciona as colunas a partir da função de combinação
            tmpColumns = permutation.getPermutationsOfRecordColumns(
                    filteredColumns, combinacao);
            // constrói a string de busca
            strBusca = seachingUtils.getStrQueryExact(tmpColumns);
            // add resultadoss
            tmpCandidates.addAll(searchCandidateRecordsFromStrQuery(strBusca,
                    HITS, recordId));

        }
        if (tmpCandidates.isEmpty() == false) {
            tmpCandidate = recordComparator.findBestCandidatePair(record,
                    tmpCandidates);

            if (tmpCandidate != null && tmpCandidate.getScore() >= 0.95) {
                return tmpCandidate;
            }
        }

        // FASE 3
        strBusca = seachingUtils.getStrQueryFuzzy(filteredColumns);
        // DO THE QUERY
        tmpCandidates = searchCandidateRecordsFromStrQuery(strBusca, HITS,
                recordId);
        // IF ANY RESULT WAS FOUND
        if (tmpCandidates.isEmpty() == false) {
            tmpCandidate = recordComparator.findBestCandidatePair(record,
                    tmpCandidates);
            return tmpCandidate;
        }
        return null;
    }

    public void close() {
        try {
            if (index != null)
                index.close();
        } catch (IOException e) {
        }
        try {
            if (reader != null)
                reader.close();
        } catch (IOException e) {
        }
    }

    @Override
    public void finalize() {
        close();
    }

    public ArrayList<RecordModel> searchCandidateRecordsFromStrQuery(
            String busca, int hits, String idCandidate) {
        int tmpDocId;
        Document tmpDocument;
        ArrayList<RecordModel> recordsFound;
        ScoreDoc[] tmpScoreDocs;

        recordsFound = new ArrayList<>();
        RecordModel tmpRecordModel;

        searcher = new IndexSearcher(reader);
        collector = TopScoreDocCollector.create(hits);
        queryParser = new QueryParser("<default field>", analyzer);
        try {
            searcher.search(queryParser.parse(busca), collector);
            //
            tmpScoreDocs = collector.topDocs().scoreDocs;

            if (tmpScoreDocs.length > 0) {
                for (ScoreDoc tmpScoreDoc : tmpScoreDocs) {
                    tmpDocId = tmpScoreDoc.doc;

                    tmpDocument = searcher.doc(tmpDocId);

                    tmpRecordModel = fromLuceneDocumentToRecord(tmpDocument);
                    seachingUtils.getStrQueryFuzzy(
                            tmpRecordModel.getColumnRecordModels());
                    seachingUtils.getStrQueryExact(
                            tmpRecordModel.getColumnRecordModels());

                    recordsFound.add(tmpRecordModel);
                }
            }

        } catch (IllegalArgumentException e) {
            StatusReporter.get().warnErrorQuery(busca,
                    ExceptionUtils.getStackTrace(e));
        } catch (IOException | ParseException e) {
        }

        return recordsFound;

    }

    private RecordModel fromLuceneDocumentToRecord(Document document) {
        ColumnRecordModel tmpRecordColumnRecord;
        String tmpValue;
        String tmpId;
        String tmpType;
        ArrayList<ColumnRecordModel> tmpRecordColumns;

        tmpRecordColumns = new ArrayList<>();
        for (ColumnConfigModel column : config.getColumns()) {
            tmpId = column.getId();
            tmpValue = document.get(tmpId);
            tmpType = column.getType();
            tmpRecordColumnRecord = new ColumnRecordModel(tmpId, tmpType,
                    tmpValue);
            tmpRecordColumnRecord.setGenerated(
                    column.isGenerated() || (tmpType.equals("copy")
                            && column.getIndexB().equals("")));
            tmpRecordColumns.add(tmpRecordColumnRecord);
        }
        RecordModel recordModel = new RecordModel(tmpRecordColumns);
        return recordModel;
    }
}
