/*
 * Copyright (c) 2019 , Kirill Ivanychev. All rights reserved.
 * Use is subject to license terms.
 *
 */

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
import java.util.HashMap;
import java.util.HashSet;

/**
 * The {@code App} class Implements price sorting algorithm
 * {@code App}.
 *
 *
 * @author  Kirill Ianychev
 * @since 1.0
 */

public class App {
    public static final boolean DEBUG = false;

    public static final int IID = 0;
    public static final int INAME = 1;
    public static final int ICONDITION = 2;
    public static final int ISTATE = 3;
    public static final int IPRICE = 4;
    public static final String SAMPLE_CSV_FILE = "./result.csv";

    // Local variables
    public static double maxPrice;
    public static double minPrice;
    public static String csvPath;
    public static HashSet<Double> pricesSet;


    /**
     * Reads prices from all file to pricesSet.
     * Alternative method for FindMinMaxPriceThread()
     * This method should be called 1st before other methods call  
     * @param
     *            
     * @return none
     */
    private static void ReadAllPrices() {
        pricesSet.clear();

        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();
    
        // Check all CSV files
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                if(App.DEBUG) System.out.println("Get prices from: " + files[i].getName());
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
                    String price = csvRecord.get(IPRICE);
                    pricesSet.add(Double.valueOf(price));
                }
            }
            catch(Exception e){
                e.printStackTrace();
            } 

        } // for(...) on all CSV files

    }
   
    /**
     * Returns Max and Min values for Price fields in a map.
     * This method should be called 1st before other methods call  
     * @param csvPath
     *            A path to the directory that contains CSV files
     * @return none
     */
    private static void FindMinMaxPriceThread(){
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
        
    }


    /**
     * Returns next price for the specified value.
     * @param currentPrice
     *            Current value of price that is taken as a reference for finding next one
     * @return value of the next price
     */
    private static double FindNextPrice(double currentPrice) {
        double nextPrice = maxPrice;

        // Check all prices from pricesSet
        for (Double price : pricesSet) {
            if(price.doubleValue() > currentPrice && price.doubleValue() < nextPrice) {
                nextPrice = price.doubleValue();
            }
        }

        return nextPrice;
    }


    /**
     * Returns a list of records for the specified price.
     *
     * @param currentPrice
     *            A price value for searching recirds
     * @return none
     */
    private static ArrayList<CSVRecord> GetRecordsForPrice(double currentPrice) {
        ArrayList<CSVRecord> recordsList = new ArrayList<CSVRecord>();
        double csvPrice;
    
        File myFolder = new File(csvPath);
        File[] files = myFolder.listFiles();
    
        // Check all files
        for (int i = 0; i < files.length; i++) {
            if (files[i].isFile()) {
                if(App.DEBUG) System.out.print("File: " + files[i].getName() + " - ");
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
                    String price = csvRecord.get(IPRICE);
                    csvPrice = Double.parseDouble(price);

                    if(csvPrice == currentPrice) {
                        recordsList.add(csvRecord);
                    }
                }

            }
            catch(Exception e){
                e.printStackTrace();
            } 

        } // for(...) on all CSV files

        return recordsList;
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
        int maxOutRecordsCnt = Integer.parseInt(args[0]);
        int maxUniqueIdRecordsCnt = Integer.parseInt(args[1]);

        // Contains IDs for the added CSV records
        HashMap<String, String> idMap = new HashMap<String, String>();
        pricesSet = new HashSet<Double>();

        // Check if the specified path exists
        csvPath = args[2];
        File csvDir = new File(csvPath);
        if(!csvDir.exists()){
            System.out.println("Please specify existing path!");
            return;
        }

        ArrayList<CSVRecord> recordsList;
        double currentPrice;
        int outRecordsCnt = 0;      // top is maxOutRecordsCnt

        System.out.print("Reading all prices...");
        ReadAllPrices();
        System.out.println(pricesSet.size() + " values");


        System.out.println("Finding prices range...");
        FindMinMaxPriceThread();
        System.out.println("Min price = " + minPrice + " Max price = " + maxPrice);

        BufferedWriter writer = Files.newBufferedWriter(Paths.get(SAMPLE_CSV_FILE));          
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                                        .withHeader("ID", "Name", "Condition", "State", "Price"));


        for(currentPrice = minPrice; currentPrice <= maxPrice; ) {
            System.out.println("Selecting Records for price " + currentPrice + " :");
            recordsList = GetRecordsForPrice(currentPrice);

            // For current price Check if the CSV records with their ID may be put to resulting file
            for(CSVRecord csvRecord : recordsList) {
                String id = csvRecord.get(IID);
                String name = csvRecord.get(INAME);
                String condition = csvRecord.get(ICONDITION);
                String state = csvRecord.get(ISTATE);
                String price = csvRecord.get(IPRICE);

                System.out.printf("%12s%32s%12s%12s%12s ", id, name, condition, state, price);
                // Check if record with such ID may be added to 

                // Create counter for ID if it does not exist
                if(!idMap.containsKey(id)) {
                    idMap.put(id, "0");
                }

                // Get counter value
                String cntValueStr = idMap.get(id);
                int cntValueInt = Integer.parseInt(cntValueStr);

                // Store new counter value for current ID
                if(cntValueInt < maxUniqueIdRecordsCnt) {
                    cntValueInt++;
                    cntValueStr = Integer.toString(cntValueInt);

                    idMap.replace(id, cntValueStr);

                    // Write the CSV record to resulting file
                    System.out.println(" - WRITTEN:" + outRecordsCnt);
                    csvPrinter.printRecord(id, name, condition, state, price);

                    outRecordsCnt++;
                    if(outRecordsCnt >= maxOutRecordsCnt) {
                        System.out.println("Finished");
                        csvPrinter.flush();
                        csvPrinter.close();  
                        return;
                    }
                }
                else {    
                    // Inform about skipped CSV record
                    System.out.println(" - SKIPPED");
                }
            }

            currentPrice = FindNextPrice(currentPrice);
            System.out.print("\r\nNext Price is " + currentPrice);

        } // for(currentPrice ...

    } // main() ...

}

//========================================================================
// PriceRangeFinderThread
// Finds min and max price vaues in the specified file
//========================================================================


/**
 * The {@code PriceRangeFinderThread} class Implements minimum and maximum
 * price finding algorithm using Thread based search. Every search is 
 * done in a separate Thread for a single .csv file
 * {@code PriceRangeFinderThread}.
 *
 *
 * @author  Kirill Ianychev
 * @since 1.0
 */
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
                    String price = csvRecord.get(App.IPRICE);
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