package sort;

import sort.controller.ExternalSorting;

public class Main {
    public static void main(String[] args) {
        int maxRows = 15;
        int keyIndex = 2;
        if (args.length > 1) {
            maxRows = Integer.parseInt(args[0]);
            keyIndex = Integer.parseInt(args[1]);
        }

        ExternalSorting externalSorting = new ExternalSorting("csv\\a.csv", keyIndex, maxRows);
        externalSorting.sort();
        externalSorting.writeToDB();
    }
}
