/**
 *
 */
package com.cidacs.rl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.cidacs.rl.config.ColumnConfigModel;

public class Cleaning {

    private static Pattern[] datePatterns = {
            Pattern.compile(
                    "(?<day>\\d{1,2})/(?<month>\\d{1,2})/(?<year>\\d{4})"),
            Pattern.compile("(?<day>\\d{1,2})(?<month>\\d{2})(?<year>\\d{4})"),
            Pattern.compile(
                    "(?<year>\\d{4})-(?<month>\\d{1,2})-(?<day>\\d{1,2})") };

    private static Pattern nameCleaningPattern = Pattern.compile("\\s+(LAUDO|BO|FF)\\s*[0-9]+");

    public static String clean(ColumnConfigModel c, String data) {
        if (data == null)
            return "";
        data = data.trim();
        switch (c.getType()) {
        case "numerical_id":
            return data.replaceAll("[^0-9]", "");
        case "name": // TODO: process names
            return nameCleaningPattern.matcher(data.toUpperCase()).replaceAll("");
        case "date": // convert ddmmyyyy and dd/mm/yyyy to yyyy-mm-dd
            for (Pattern p : datePatterns) {
                Matcher m = p.matcher(data);
                if (m.matches()) {
                    return m.group("year") + '-'
                            + StringUtils.leftPad(m.group("month"), 2, '0')
                            + '-' + StringUtils.leftPad(m.group("day"), 2, '0');
                }
            }
            return data;
        case "ibge":
            if (data.length() == 7) // remove check digit
                return data.substring(0, 6);
            return data;
        case "categorical":
        case "string":
        default:
            return data;
        }
    }

}
