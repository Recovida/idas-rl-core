package recovida.idas.rl.core.io;

import java.text.DecimalFormatSymbols;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import recovida.idas.rl.core.lang.MessageProvider;

/**
 * Represents separator characters.
 */
public enum Separator {

    /**
     * The default column separator in the current language.
     */
    DEFAULT_COL_SEP(null, Separator::columnSeparatorFromLocale),

    /**
     * The default decimal separator in the current language.
     */
    DEFAULT_DEC_SEP(null, Separator::decimalSeparatorFromLocale),

    /**
     * A comma (",").
     */
    COMMA(','),

    /**
     * A colon (":").
     */

    COLON(':'),

    /**
     * A semicolon (";").
     */
    SEMICOLON(';'),

    /**
     * A tab character.
     */
    TAB('\t'),

    /**
     * A dot (".").
     */
    DOT('.'),

    /**
     * A vertical bar ("|").
     */
    PIPE('|');

    private Character character;
    private LocaleToSeparator lts;

    Separator(char character) {
        this.character = character;
    }

    /**
     * Abstraction for a function that returns a (column/decimal) separator for
     * a given language.
     */
    public interface LocaleToSeparator {

        /**
         * Obtains the separator that corresponds to the given locale.
         * 
         * @param l the locale
         * @return the separator for {@code l}
         */
        Separator localeToSeparator(Locale l);
    }

    Separator(Character character, LocaleToSeparator lts) {
        this.character = character;
        this.lts = lts;
    }

    @Override
    public String toString() {
        String name = MessageProvider.getMessage(
                "separators." + name().replaceAll("_", "").toLowerCase());
        return character == null ? MessageFormat.format(name,
                lts.localeToSeparator(MessageProvider.getLocale()).toString()
                        .toLowerCase())
                : name;
    }

    public char getCharacter() {
        return character != null ? character
                : lts.localeToSeparator(MessageProvider.getLocale())
                        .getCharacter();
    }

    public static List<Separator> getColumnSeparators() {
        return Arrays.asList(DEFAULT_COL_SEP, SEMICOLON, COMMA, COLON, PIPE,
                TAB);
    }

    public static List<Separator> getDecimalSeparators() {
        return Arrays.asList(DEFAULT_DEC_SEP, DOT, COMMA);
    }

    /**
     * Returns a separator from its name.
     * 
     * @param name name of the separator
     * @return a {@code Separator} instance correspondent to {@code name}, or
     *         {@code null} if it does not exist
     */
    public static Separator fromName(String name) {
        if (name == null)
            return null;
        try {
            return Separator.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Separator decimalSeparatorFromLocale(Locale l) {
        char sep = new DecimalFormatSymbols(l).getDecimalSeparator();
        if (sep == ',')
            return COMMA;
        return DOT;
    }

    private static Separator columnSeparatorFromLocale(Locale l) {
        char sep = new DecimalFormatSymbols(l).getDecimalSeparator();
        if (sep == ',')
            return SEMICOLON;
        return COMMA;
    }

}
