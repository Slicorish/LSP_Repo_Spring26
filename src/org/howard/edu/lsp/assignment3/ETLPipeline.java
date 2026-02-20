package org.howard.edu.lsp.assignment3;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.*;

public class ETLPipeline{

    public static void main(String[] args) throws IOException {

        CSVReader reader = new CSVReader();
        CSVWriter writer = new CSVWriter();
        ProductParser parser = new ProductParser();

        List<String> lines;

        try {
            lines = reader.read("data/products.csv");
        } catch (NoSuchFileException e) {
            System.err.println("Error: File not found.");
            return;
        }

        List<String> output = new ArrayList<>();
        output.add(lines.get(0) + ",Price Range");

        int processed = 0;
        int skipped = 0;

        for (int i = 1; i < lines.size(); i++) {
            processed++;

            Optional<Product> optionalProduct = parser.parse(lines.get(i));

            if (optionalProduct.isEmpty()) {
                skipped++;
                continue;
            }

            Product product = optionalProduct.get();

            product.transform();

            output.add(product.toCSV());
        }

        writer.write("data/transformed_products.csv", output);

        System.out.println("Processed: " + processed);
        System.out.println("Successful: " + (processed - skipped));
        System.out.println("Skipped: " + skipped);
    }
}
