package recovida.idas.rl.core.util;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class Phonetic {

    public static class Substitution {

        public Pattern pattern;

        public String replacement;

        public Substitution(Pattern pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }
    }

    public static class MultipleSubstitution {

        private final List<Substitution> items = new LinkedList<>();

        public MultipleSubstitution add(String regex, String replacement) {
            items.add(new Substitution(Pattern.compile(regex), replacement));
            return this;
        }

        public String apply(String s) {
            for (Substitution p : items)
                s = p.pattern.matcher(s).replaceAll(p.replacement);
            return s;
        }

    }

    static final MultipleSubstitution globalSubstitutions = new MultipleSubstitution()
            .add(" (D?[AEO]|D[AO]S|EM|N[OA]S?) ", " ").add("(.)(\\1)+", "$1");
    
    static final MultipleSubstitution perNameSubstitutions = new MultipleSubstitution()
            .add("Y", "I").add("PH", "F").add("CHR", "CR").add("CHIO", "QUIO")
            .add("CÇ", "S").add("C([TS])", "$1").add("[MN]Ç", "S")
            .add("[MN]([BCDFGJKLPQRSTVWXZ])", "$1").add("SÇ", "C")
            .add("S([BCDFGJKLMNPQRTVWXZ])", "$1")
            .add("([BCDFGKMPTVWZ])[LR]", "$1").add("MR", "M")
            .add("([BCDFGPQST])([BCÇDFJMNPSTVWXZ])", "$1I$2")
            .add("([BCDFGPQST])([GQ])", "$1UI$2").add("[MR]G", "G")
            .add("G([EI])", "J$1").add("[RMN]J|GU", "J").add("G[RL]", "G")
            .add("C([EI])", "S$1").add("CH", "S").add("QU?|C([AOU])|CK?", "K$1")
            .add("LH", "L").add("RM", "SM").add("N|[GRS]M|MD", "M")
            .add("AO", "AM").add("([AEIO])L([BCÇDFGJKMNPQRSTVWXZ]|$)", "$1U$2")
            .add("NH", "N").add("PR", "P").add("([ÇXCZ]|RS)", "S")
            .add("T[LR]|[LRS]T", "T").add("U", "O").add("W", "V").add("L", "R")
            .add("H", "").add("I", "E").add("([DLMNRSTUZ]|AO)$", "")
            .add("K$", "KE").add("(.)(\\1)+", "$1");

    public static String convert(String name) {
        if (name == null)
            return "";
        name = StringUtils.stripAccents(name.toUpperCase());
        name = globalSubstitutions.apply(name);
        List<String> cleaned_name = new LinkedList<>();
        for (String word : name.split("\\s+"))
            cleaned_name.add(perNameSubstitutions.apply(word));
        return StringUtils.join(cleaned_name, ' ').replaceAll("\\s\\s+", " ");
    }

}
