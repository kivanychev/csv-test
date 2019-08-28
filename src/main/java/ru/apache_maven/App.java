package ru.apache_maven;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.Reader;
import java.io.BufferedWriter;
import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.swing.event.MouseInputListener;

public class App {
    public static final boolean DEBUG = false;

    public static final int IID = 0;
    public static final int INAME = 1;
    public static final int ICONDITION = 2;
    public static final int ISTATE = 3;
    public static final int IPRIICE = 4;
    public static final String SAMPLE_CSV_FILE = "./result.csv";

    // Local variables
    public static double maxPrice;
    public static double minPrice;

     /**
     * Returns a value by {@link Enum}.
     *
     * @param e
     *            an enum
     * @return the String at the given enum String
     */
    private void getNextRecords(int currentPrice){
        return;
    }

    /**
     * Returns Max and Min values for Price fields in a map.
     *
     * @param csvPath
     *            A path to the directory that contains CSV files
     * @return none
     */
    private static void FindMinMaxPriceThread(String csvPath){
        PriceRangeFinderThread finderThread;
        ArrayList<PriceRangeFinderThread> threadArray = new ArrayList<PriceRangeFinderThread>();
        
        // Get number of files to create Threads list
        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();
        
        // Start threads for all files
        for (int i = 0; i < files.length; i++) {

            if (!files[i].isFile()) {
                continue;
            }

            finderThread = new PriceRangeFinderThread();
            if(finderThread.SetCsvFilePath(csvPath + "/" + files[i].getName())) {
                threadArray.add(finderThread);
                finderThread.start();
            }
        }

        maxPrice = 0;
        minPrice = threadArray.get(0).GetMinPrice();

        // Check all threads values
        for (PriceRangeFinderThread priceThread : threadArray) {
            if(priceThread.GetMaxPrice() > maxPrice) {
                maxPrice = priceThread.GetMaxPrice();
            }

            if(priceThread.GetMinPrice() < minPrice) {
                minPrice = priceThread.GetMinPrice();
            }   
        }
        System.out.println("Min price = " + minPrice + " Max price = " + maxPrice);

        
    }

    /**
     * Returns Max and Min values for Price fields in a map.
     *
     * @param csvPath
     *            A path to the directory that contains CSV files
     * @return none
     */
    private static void findMinMaxPrice(String csvPath) {
        double price_double;

        maxPrice = 0.0;
        minPrice = -1.0;

        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();
        
        // Check all files
        for (int i = 0; i < files.length; i++) {

            if (!files[i].isFile()) {
                continue;
            }
    
            // Read single CSV file
            try ( Reader reader = Files.newBufferedReader(Paths.get(csvPath + "/" + files[i].getName()));
                  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
                ) {

                // Find in current CSV file
                for (CSVRecord csvRecord : csvParser) {
                    String price = csvRecord.get(IPRIICE);
                    price_double = Double.parseDouble(price);

                    if(minPrice == -1.0){
                        minPrice = price_double;
                        System.out.println("\r\nFirst time minPrice = " + minPrice);
                    }

                    if(price_double > maxPrice) {
                        maxPrice = price_double;
                        System.out.println("Max price changed to: " + maxPrice + " in " + files[i].getName());
                    }

                    if(price_double < minPrice) {
                        minPrice = price_double;
                        System.out.println("Min price changed to: " + minPrice + " in " + files[i].getName());
                    }
                } // for(...) on a single CSV file

            } 
            catch(Exception e){
                e.printStackTrace();
            }
        } // for(...) on all CSV files
        System.out.println("Min price: " + minPrice + "  Max price: "+ maxPrice);

    }


    /**
     * Program entry point.
     *
     * @param args
     *            Arguments: <maxRecords> <maxUniqIdRecords> <csv folderpath>
     * @return none
     */    
    public static void main(String[] args) throws IOException {
        
        // Check parameters count
        if(args.length < 3) {
            System.out.println("Specify 3 Parameters:\r\n<goods amount to select>\r\n<same ID goods count>\r\n<path to csv dir>\r\n");
            return;
        }

        // Read application parameters
        int maxOutRecords = Integer.parseInt(args[0]);
        int maxUniqueRecords = Integer.parseInt(args[1]);

        // Check if the specified path exists
        String csvPath = args[2];
        File csvDir = new File(csvPath);
        if(!csvDir.exists()){
            System.out.println("Please specify existing path!");
            return;
        }

        FindMinMaxPriceThread(csvPath);


        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();
        
        String searchID = "378538";
        int foundRecords = 0;

        BufferedWriter writer = Files.newBufferedWriter(Paths.get(SAMPLE_CSV_FILE));          
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                                        .withHeader("ID", "Name", "Condition", "State", "Price"));

        // Check all files
        for (int i = 0; i < files.length; i++) {
            int recordsCnt = 0;

            if (files[i].isFile()) {
                System.out.print("File: " + files[i].getName() + " - ");
            } else {
                continue;
            }
    
            // Read single CSV file
            try ( Reader reader = Files.newBufferedReader(Paths.get(csvPath + "/" + files[i].getName()));
                  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
                ) {

        

                // Find in current CSV file
                for (CSVRecord csvRecord : csvParser) {

                    // Accessing Values by Column Index
                    String id = csvRecord.get(IID);
                    String name = csvRecord.get(INAME);
                    String condition = csvRecord.get(ICONDITION);
                    String state = csvRecord.get(ISTATE);
                    String price = csvRecord.get(IPRIICE);

                    // Read
                    if(id.equals(searchID)) {
                        csvPrinter.printRecord(id, name, condition, state, price);
                        foundRecords++;
                    }

                    recordsCnt++;
                } // for(...) on a single CSV file

                System.out.println(recordsCnt + " records");
            } 

        } // for(...) on all CSV files

        csvPrinter.flush();
        csvPrinter.close();            

        System.out.println("Found records: " + foundRecords);

    }

}

//========================================================================
// PriceRangeFinder
// Finds min and max price vaues in the specified file
//========================================================================

class PriceRangeFinderThread extends Thread {
    private volatile boolean busy;
    private double minPrice;
    private double maxPrice;
    private String csvFilePath;

    @Override
    public void run(){
        busy = true;

        if(App.DEBUG) System.out.println(currentThread().getName() + " started for " + csvFilePath);

        if(csvFilePath.equals("")) {
            System.out.println(currentThread().getName() + " Error: Unable to open file: " + csvFilePath);
            return;
        }

        minPrice = -1.0;
        maxPrice = 0.0;
        double price_double;

            // Read single CSV file
            try ( Reader reader = Files.newBufferedReader(Paths.get(csvFilePath));
                  CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase()
                    .withTrim());
                ) {

                // Find in current CSV file
                for (CSVRecord csvRecord : csvParser) {
                    String price = csvRecord.get(App.IPRIICE);
                    price_double = Double.parseDouble(price);

                    if(minPrice == -1.0){
                        minPrice = price_double;
                        if(App.DEBUG) System.out.println(currentThread().getName() + "First time minPrice = " + minPrice);
                    }

                    if(price_double > maxPrice) {
                        maxPrice = price_double;
                        if(App.DEBUG) System.out.println(currentThread().getName() + "Max price changed to: " + maxPrice + " in " + csvFilePath);
                    }

                    if(price_double < minPrice) {
                        minPrice = price_double;
                        if(App.DEBUG) System.out.println(currentThread().getName() + "Min price changed to: " + minPrice + " in " + csvFilePath);
                    }
                } // for(...) on a single CSV file

            } 
            catch(Exception e){
                e.printStackTrace();
            }

        busy = false;
        if(App.DEBUG) System.out.println(currentThread().getName() + " Done! minPrice=" + minPrice + " maxPrice=" + maxPrice+ " in " + csvFilePath);
    }


    public boolean SetCsvFilePath(String fname) {
        File file;

        csvFilePath = fname;

        try
        {
            file = new File(csvFilePath);
        }
        catch(Exception e) {
            csvFilePath = "";
            System.out.println(currentThread().getName() +  ": Error opening file: " + csvFilePath);

            e.printStackTrace();
            return false;
        }

        if(!file.isFile()) {
            System.out.println("Error: This is not a valid filename:" + csvFilePath);
            return false;
        } 

        return true;
    }


    public double GetMaxPrice() {
        while(busy){
            try {
                sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return maxPrice;
    }


    public double GetMinPrice() {
        while(busy){
            try {
                sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        return minPrice;
    }

}