package com.zhuinden.sparkexperiment.utils;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HandleString {
    public static void main(String[] args) {
        Scanner scan = new Scanner(System.in);
        String c;
        if ((c = scan.nextLine()) != null) {
            Pattern pt = Pattern.compile("[^a-zA-Z0-9]");
            Matcher match = pt.matcher(c);
            while (match.find()) {
                c = c.replace(Character.toString(c.charAt(match.start())), "");
            }
            System.out.println(c);
        }
    }
}
