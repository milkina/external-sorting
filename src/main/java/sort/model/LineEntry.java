package sort.model;

import java.util.Arrays;

public class LineEntry implements Comparable<LineEntry> {
    private String[] columns;
    private static int keyIndex;

    public LineEntry(String[] columns) {
        this.columns = columns;
    }

    @Override
    public int compareTo(LineEntry o) {
        return this.columns[keyIndex - 1].compareTo(o.columns[keyIndex - 1]);
    }

    public String[] getColumns() {
        return columns;
    }

    public void setColumns(String[] columns) {
        this.columns = columns;
    }

    public static void setKeyIndex(int keyIndex) {
        LineEntry.keyIndex = keyIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LineEntry lineEntry = (LineEntry) o;
        return Arrays.equals(columns, lineEntry.columns);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(columns);
    }

    @Override
    public String toString() {
        return "LineEntry{" +
                "columns=" + Arrays.toString(columns) +
                '}';
    }
}
