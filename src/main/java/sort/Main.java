package sort;

import sort.controller.ExternalSorting;

public class Main {
    public static void main(String[] args) {
        int maxRows = 3;
        int keyIndex = 2;

        ExternalSorting externalSorting = new ExternalSorting("csv\\a.csv", keyIndex, maxRows);
        externalSorting.sort();
        externalSorting.writeToDB();
    }
}
