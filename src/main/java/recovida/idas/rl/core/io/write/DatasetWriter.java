package recovida.idas.rl.core.io.write;

public interface DatasetWriter {

    boolean writeRow(String row);

    void close();

}
