package recovida.idas.rl.core.io;

import java.text.DecimalFormatSymbols;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import recovida.idas.rl.core.lang.MessageProvider;

public enum Separator {

    DEFAULT_COL_SEP(null, Separator::columnSeparatorFromLocale),
    DEFAULT_DEC_SEP(null, Separator::decimalSeparatorFromLocale), COMMA(','),
    COLON(':'), SEMICOLON(';'), TAB('\t'), DOT('.');

    private Character character;
    private LocaleToSeparator lts;

    Separator(char character) {
        this.character = character;
    }

    public interface LocaleToSeparator {
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
        return character;
    }

    public static List<Separator> getColumnSeparators() {
        return Arrays.asList(DEFAULT_COL_SEP, SEMICOLON, COMMA, COLON, TAB);
    }

    public static List<Separator> getDecimalSeparators() {
        return Arrays.asList(DEFAULT_DEC_SEP, DOT, COMMA);
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
