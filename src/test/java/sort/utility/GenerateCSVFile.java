package sort.utility;

import sort.io.CSVWriter;
import sort.model.LineEntry;

import java.io.IOException;

public class GenerateCSVFile {
    public static final int VALUE_MAX_LENGTH = 20;

    public void generateFile(int rowsNumber, int columnsNumber, String fileName) {
        try (CSVWriter csvWriter = new CSVWriter(fileName)) {
            for (int i = 0; i < rowsNumber; i++) {
                String[] columns = generateColumns(columnsNumber);
                LineEntry lineEntry = new LineEntry(columns);
                csvWriter.write(lineEntry);
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private String[] generateColumns(int columnsNumber) {
        String[] columns = new String[columnsNumber];
        for (int j = 0; j < columnsNumber; j++) {
            columns[j] = generateValue();
        }
        return columns;
    }

    private String generateValue() {
        double length = Math.random() * VALUE_MAX_LENGTH;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char symbol = generateSymbol('a', 'z');
            stringBuilder.append(symbol);
        }
        return stringBuilder.toString();
    }

    private char generateSymbol(int min, int max) {
        max -= min;
        int code = (int) (Math.random() * max + min);
        return (char) (code);
    }
}
