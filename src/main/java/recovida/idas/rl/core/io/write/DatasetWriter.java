package recovida.idas.rl.core.io.write;

/**
 * Provides a mechanism to write a dataset to a file.
 */
public interface DatasetWriter extends AutoCloseable {

    /**
     * 
     * Writes a row to the file.
     * 
     * @param row the row to be written
     * @return whether the row was successfully written
     */
    boolean writeRow(String row);

    @Override
    void close();

}
