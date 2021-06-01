package recovida.idas.rl.core.lang;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

/**
 * A class that deals with i18n and provides translated messages.
 */
public class MessageProvider {

    /**
     * List of supported language tags.
     */
    public static final List<String> SUPPORTED_LANGUAGES = readAvailableLanguages();

    /**
     * The language tag of the system language, if present
     * {@link #SUPPORTED_LANGUAGES}; or the some language tag in
     * {@link #SUPPORTED_LANGUAGES} that matches the system language (even if
     * the country does not match), if available; or the first language on the
     * list, as a fallback.
     */
    public static final String DEFAULT_LANGUAGE = getBestLanguage();

    protected static Locale currentLocale = Locale
            .forLanguageTag(DEFAULT_LANGUAGE);

    protected static ResourceBundle bundle = createBundle(currentLocale);

    public static void setLocale(Locale locale) {
        bundle = createBundle(locale);
        currentLocale = locale;
    }

    public static Locale getLocale() {
        return currentLocale;
    }

    protected static List<String> readAvailableLanguages() {
        try {
            return Collections.unmodifiableList(IOUtils
                    .readLines(
                            MessageProvider.class.getClassLoader()
                                    .getResourceAsStream("lang/languages.txt"),
                            Charset.forName("UTF-8"))
                    .stream().filter(l -> !l.isEmpty())
                    .collect(Collectors.toList()));
        } catch (IOException e) {
            System.err.println(
                    "Could not read language files. The installation is corrupt.");
            System.err.flush();
            System.exit(1);
            return null; // to avoid warning
        }
    }

    protected static String getBestLanguage() {
        Locale defaultLocale = Locale.getDefault();
        // try exact match
        int idx = SUPPORTED_LANGUAGES.indexOf(defaultLocale.toLanguageTag());
        if (idx < 0) // try language match, ignoring country
            idx = SUPPORTED_LANGUAGES.stream().map(x -> x.split("-", 2)[0])
                    .collect(Collectors.toList())
                    .indexOf(defaultLocale.getLanguage());
        // as a last resort, use the first language as a fallback
        return SUPPORTED_LANGUAGES.get(Math.max(0, idx));
    }

    protected static ResourceBundle createBundle(Locale locale) {
        return ResourceBundle.getBundle("lang.core_messages", locale);
    }

    public static String getMessage(String key) {
        try {
            return bundle.getString(key);
        } catch (MissingResourceException e) {
            return "?????";
        }
    }

}
