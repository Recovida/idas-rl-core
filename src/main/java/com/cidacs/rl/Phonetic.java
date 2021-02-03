package com.cidacs.rl;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.sun.tools.javac.util.Pair;

public class Phonetic {

    public static class Substitution {

        private List<Pair<Pattern, String>> items = new LinkedList<>();

        public Substitution add(String regex, String replacement) {
            items.add(new Pair<>(Pattern.compile(regex), replacement));
            return this;
        }

        public String apply(String s) {
            for (Pair<Pattern, String> p : items)
                s = p.fst.matcher(s).replaceAll(p.snd);
            return s;
        }

    }

    final static Substitution globalSubstitutions = new Substitution()
            .add(" (D?[AEO]|D[AO]S|EM|N[OA]S?) ", " ").add("(.)(\\1)+", "$1");
    final static Substitution perNameSubstitutions = new Substitution()
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
        name = StringUtils.stripAccents(name.toUpperCase());
        name = globalSubstitutions.apply(name);
        List<String> cleaned_name = new LinkedList<>();
        for (String word : name.split("\\s+"))
            cleaned_name.add(perNameSubstitutions.apply(word));
        return StringUtils.join(cleaned_name, ' ').replaceAll("\\s\\s+", " ");
    }

}
