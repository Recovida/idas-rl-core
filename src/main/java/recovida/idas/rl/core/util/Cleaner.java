package recovida.idas.rl.core.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import recovida.idas.rl.core.config.ColumnConfigModel;

/**
 * This class contains some methods that are used to clean strings before the
 * linkage.
 */
public class Cleaner {

    private static Pattern[] datePatterns = {
            Pattern.compile(
                    "(?<day>\\d{1,2})/(?<month>\\d{1,2})/(?<year>\\d{4})"),
            Pattern.compile("(?<day>\\d{1,2})(?<month>\\d{2})(?<year>\\d{4})"),
            Pattern.compile(
                    "(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2})") };

    private Pattern nameCleaningPattern = Pattern.compile("");

    /**
     * Cleans a string by transforming and removing characters according to the
     * column type.
     * 
     * @param c    the column definition
     * @param data the string to be cleaned
     * @return the cleaned string
     */
    public String clean(ColumnConfigModel c, String data) {
        if (data == null)
            return "";
        if (c.getType().equals("copy"))
            return data;
        data = data.trim();
        switch (c.getType()) {
        case "numerical_id":
            return data.replaceAll("[^0-9]", "");
        case "name":
            return nameCleaningPattern
                    .matcher(StringUtils.stripAccents(data.toUpperCase()))
                    .replaceAll("").trim();
        case "date": // convert ddmmyyyy and yyyy-mm-dd to dd/mm/yyyy
            for (Pattern p : datePatterns) {
                Matcher m = p.matcher(data);
                if (m.matches()) {
                    return StringUtils.leftPad(m.group("day"), 2, '0') + '/'
                            + StringUtils.leftPad(m.group("month"), 2, '0')
                            + '/' + m.group("year");
                }
            }
            return data;
        case "ibge":
            data = data.replaceAll("[^0-9]", "");
            if (data.length() == 7) // remove check digit
                return data.substring(0, 6);
            return data;
        case "categorical":
        case "string":
        default:
            return data;
        }
    }

    public Pattern getNameCleaningPattern() {
        return nameCleaningPattern;
    }

    public void setNameCleaningPattern(Pattern pattern) {
        nameCleaningPattern = pattern == null ? Pattern.compile("") : pattern;
    }

    public void setNameCleaningPattern(String pattern) {
        nameCleaningPattern = Pattern.compile(pattern == null ? "" : pattern);
    }

}
