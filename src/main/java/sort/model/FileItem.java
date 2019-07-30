package sort.model;

import java.util.Objects;

public class FileItem {
    private int rowNumber;
    private String name;
    private int currentRow;

    public FileItem(int rowNumber, String name) {
        this.rowNumber = rowNumber;
        this.name = name;
    }

    public int getRowNumber() {
        return rowNumber;
    }

    public void setRowNumber(int rowNumber) {
        this.rowNumber = rowNumber;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCurrentRow() {
        return currentRow;
    }

    public void setCurrentRow(int currentRow) {
        this.currentRow = currentRow;
    }

    public void increaseCurrentRow() {
        this.currentRow++;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileItem fileItem = (FileItem) o;
        return rowNumber == fileItem.rowNumber &&
                currentRow == fileItem.currentRow &&
                Objects.equals(name, fileItem.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowNumber, name, currentRow);
    }

    @Override
    public String toString() {
        return "FileItem{" +
                "rowNumber=" + rowNumber +
                ", name='" + name + '\'' +
                ", currentRow=" + currentRow +
                '}';
    }
}
