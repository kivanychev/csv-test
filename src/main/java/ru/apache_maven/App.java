package ru.apache_maven;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;

public class App {
    private static final String SAMPLE_CSV_FILE_PATH = "F:/Projects/Test/Maven/project1/test/csv/csv1.csv";

    private static final int IID = 0;
    private static final int INAME = 1;
    private static final int ICONDITION = 2;
    private static final int ISTATE = 3;
    private static final int IPRIICE = 4;

    public static void main(String[] args) throws IOException {

        // Check parameters count
        if(args.length < 3) {
            System.out.println("Specify 3 Parameters:\r\n<goods amount to select>\r\n<same ID goods count>\r\n<path to csv dir>\r\n");
            return;
        }

        // Check if the specified path exists
        String csvPath = args[2];
        File csvDir = new File(csvPath);
        if(!csvDir.exists()){
            System.out.println("Please specify existing path!");
            return;
        }


        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();

        for (int i = 0; i < files.length; i++) {
            int recordsCnt = 0;

            if (files[i].isFile()) {
                System.out.print("File: " + files[i].getName() + " - ");
            } else {
                continue;
            }

            try ( Reader reader = Files.newBufferedReader(Paths.get(csvPath + "/" + files[i].getName()));
                  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                ) {
        
                for (CSVRecord csvRecord : csvParser) {
                    // Accessing Values by Column Index

                    String id = csvRecord.get(IID);
                    String name = csvRecord.get(INAME);
                    String condition = csvRecord.get(ICONDITION);
                    String state = csvRecord.get(ISTATE);
                    String price = csvRecord.get(IPRIICE);
                    
                    recordsCnt++;
                }
                System.out.println(recordsCnt + " records");

            }


        }
    }
}