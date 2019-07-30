package sort.io;

import sort.model.LineEntry;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVReader implements Closeable {
    public static final String DELIMITER = ";";
    private BufferedReader br;
    private String fileName;

    public CSVReader(String fileName) throws FileNotFoundException {
        this.fileName = fileName;
        this.br = new BufferedReader(new FileReader(fileName));
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<LineEntry> read(int maxLines) {
        String line = "";
        int i = 0;
        List<LineEntry> list = new ArrayList<>();
        try {
            while (i++ < maxLines) {
                line = br.readLine();
                if (line == null) {
                    break;
                }
                String[] columns = line.split(DELIMITER);
                LineEntry lineEntry = new LineEntry(columns);
                list.add(lineEntry);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }

    public LineEntry readLine(int lineIndex) {
        List<LineEntry> list = read(lineIndex);
        if (list.isEmpty()) {
            return null;
        } else {
            return list.get(list.size() - 1);
        }
    }

    @Override
    public void close() throws IOException {
        if (br != null) {
            br.close();
        }
    }
}
