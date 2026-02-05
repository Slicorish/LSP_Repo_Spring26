package org.howard.edu.lsp.assignment2;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class ETLPipeline {
    public static void main(String[] args) throws IOException {
        // Read input CSV from the project's data folder, includes try catch for if file path can't be found or it's missing
           List<String> lines;
        try {
            lines = Files.readAllLines(Paths.get("data/products.csv"));

        } catch (NoSuchFileException e) {
            System.err.println("Error: data/products.csv not found, program missing an input file and will terminate.");
            return; // stops the program
        }

        // initializing output to write to resulting file, transformed_products.csv
        List <String> output = new ArrayList<>();

        // initializing counters for rows processed and rows skipped 
        int rowsProcessed = 0;
        int rowsSkipped = 0;

        // Add header for new column for Price Range
        String header = lines.get(0);
        output.add(header + ",Price Range");

        // loops through the rows of the file and splits for formatting 
        for(int row = 1; row < lines.size(); row++){
            String line = lines.get(row);
            String[] fields = line.split(",");

        // Condition for if there are 4 fields 
            if (fields.length != 4) {
                rowsProcessed++;
                rowsSkipped++;
                continue;
            }
        // condition for if product ID can be parsed (is it a valid ID number)
            String productId = fields[0];
            try {
                Integer.parseInt(productId.trim());
            }
            catch(NumberFormatException e) {
                rowsProcessed++;
                rowsSkipped++;
                continue;
            }
            String productName = fields[1];
            String price = fields[2];

            // condition for if price can be parsed (is it a valid price)
            double priceDecimal;
            try {
                priceDecimal = Double.parseDouble(price.trim());
            } catch (NumberFormatException e) {
                rowsProcessed++;
                rowsSkipped++;
                continue;
            }

            String category = fields[3];
            category = category.trim(); // Remove leading/trailing whitespace

            // condition for if any of the fields are empty
              if (productId.isEmpty() || productName.isEmpty() || price.isEmpty() || category.isEmpty()) {
                rowsProcessed++;
                rowsSkipped++;
                continue;
            } 
            productName = productName.trim().toUpperCase();  //Step #1

            // Step #2 
            if(category.equals("Electronics")) {
                priceDecimal = priceDecimal * 0.9; // Apply 10% discount
            }
            
            double priceRounded = Math.round(priceDecimal * 100.0) / 100.0; // Round to 2 decimal places

            // Step #3 
            if(priceRounded > 500.00 && category.equals("Electronics")) {
                category = "Premium Electronics"; 
            }
            price = String.format("%.2f", priceRounded);

            // logic for determining the value of the new row, PriceRange
            String priceRange;
            if(priceRounded <= 10.00) {
                priceRange = "Low";
            } else if(priceRounded <= 100.00) {
                priceRange = "Medium";
            } else if (priceRounded <= 500.00) {
                priceRange = "High";
            }
            else{
                priceRange = "Premium";
            }
            rowsProcessed++;
            
            
            String newLine = String.join(",",
                productId,
                productName,
                price,
                category,
                priceRange
            );
            output.add(newLine);
        }
        //output for counting of rows processed, rows skipped, and rows changed
        Files.write(Paths.get("data/transformed_products.csv"), output);
        System.out.println("Count of total rows processed: " + rowsProcessed);
        System.out.println("Count of rows successfully transformed: " + (rowsProcessed - rowsSkipped));
        System.out.println("Count of rows skipped due to formatting errors: " + rowsSkipped);
        System.out.println("Output written to: " + Paths.get("data/transformed_products.csv").toAbsolutePath());
    }
}
