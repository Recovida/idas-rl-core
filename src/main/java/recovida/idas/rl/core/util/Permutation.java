package recovida.idas.rl.core.util;

import java.util.ArrayList;

// CHECKSTYLE.OFF: Javadoc*

// CHECKSTYLE.OFF: MissingJavadocMethod
// CHECKSTYLE.OFF: SummaryJavadoc
// CHECKSTYLE.OFF: WriteTag

import java.util.List;

import recovida.idas.rl.core.record.ColumnRecordModel;

public class Permutation {

    public ArrayList<ArrayList<Integer>> combine(int n, int k) {
        ArrayList<ArrayList<Integer>> result = new ArrayList<>();

        if (n <= 0 || n < k)
            return result;

        ArrayList<Integer> item = new ArrayList<>();
        dfs(n, k, 1, item, result); // because it need to begin from 1

        return result;
    }

    private void dfs(int n, int k, int start, ArrayList<Integer> item,
            ArrayList<ArrayList<Integer>> res) {
        if (item.size() == k) {
            res.add(new ArrayList<>(item));
            return;
        }

        for (int i = start; i <= n; i++) {
            item.add(i);
            dfs(n, k, i + 1, item, res);
            item.remove(item.size() - 1);
        }
    }

    public List<ColumnRecordModel> getPermutationsOfRecordColumns(
            List<ColumnRecordModel> columns, List<Integer> indexes) {
        ArrayList<ColumnRecordModel> tmpResultColumns = new ArrayList<>();

        // filterOff unwanted columns
        for (Integer i : indexes) {
            // -1 because indexes starts from 1
            // Java is 0 indexed.
            tmpResultColumns.add(columns.get(i - 1));
        }

        return tmpResultColumns;
    }

}
