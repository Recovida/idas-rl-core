package recovida.idas.rl.core.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import recovida.idas.rl.core.record.ColumnRecordModel;

public class SearchingUtils {

    public String addTildeToEachName(String name) {
        String[] tmp;
        tmp = name.split(" ");

        String result = "";

        for (String strPiece : tmp) {
            result = result + strPiece + "~ ";
        }
        return result.substring(0, result.length() - 1);
    }

    public String getStrQueryFuzzy(Collection<ColumnRecordModel> columns) {
        StringBuilder query = new StringBuilder(new String());

        for (ColumnRecordModel column : columns) {
            if (column.getType().equals("string")) {
                // just in case we need it
            }
            if (column.getType().equals("name") || column.getType().equals("gender")) {
                // query = query + "+" +c.getName() + ":\"" + r[c.getColumn()] +
                // "\" ";
                if (column.getValue().isEmpty() == false) {
                    query.append(column.getId()).append(":(")
                            .append(addTildeToEachName(column.getValue()))
                            .append(") ");
                }
            }
            if (column.getType().equals("date")) {
                if (column.getValue().isEmpty() == false) {
                    query.append(column.getId()).append(":")
                            .append(column.getValue()).append("~ ");
                }
            }
            if (column.getType().equals("ibge") || column.getType().equals("numerical_id")) {
                if (column.getValue().isEmpty() == false) {
                    query.append(column.getId()).append(":")
                            .append(column.getValue()).append("~1 ");
                }
            }
            if (column.getType().equals("categorical")) {
                if (column.getValue().isEmpty() == false) {
                    query.append(column.getId()).append(":(")
                            .append(column.getValue()).append("~) ");
                }
            }
        }
        return query.toString();
    }

    public String getStrQueryExact(Collection<ColumnRecordModel> columns) {
        StringBuilder query = new StringBuilder(new String());

        for (ColumnRecordModel column : columns) {
            if (!column.getType().equals("copy")
                    && !column.getValue().isEmpty()) {
                if (column.getType().equals("name") || column.getType().equals("gender")) {
                    query.append("+").append(column.getId()).append(":\"")
                            .append(column.getValue()).append("\" ");
                }
                if (column.getType().equals("date")) {
                    query.append("+").append(column.getId()).append(":\"")
                            .append(column.getValue()).append("\" ");
                }
                if (column.getType().equals("ibge") || column.getType().equals("numerical_id")) {
                    query.append("+").append(column.getId()).append(":")
                            .append(column.getValue()).append("~1 ");
                }
                if (column.getType().equals("categorical")) {
                    query.append("+").append(column.getId()).append(":\"")
                            .append(column.getValue()).append("\" ");
                }
            }
        }
        return query.toString();
    }

    public List<ColumnRecordModel> filterUnusedColumns(
            Collection<ColumnRecordModel> columns) {
        ArrayList<ColumnRecordModel> tmpResult = new ArrayList<>();

        for (ColumnRecordModel column : columns) {
            switch (column.getType()) {
            case "name":
                tmpResult.add(column);
                break;
            case "date":
                tmpResult.add(column);
                break;
            case "ibge":
                tmpResult.add(column);
                break;
            case "categorical":
                tmpResult.add(column);
                break;
            case "numerical_id":
                tmpResult.add(column);
                break;
            case "gender":
                tmpResult.add(column);
                break;
            }
        }
        return tmpResult;
    }
}
