package org.howard.edu.lsp.assignment3;

import java.util.Optional;

/* ProductParser is responsible for parsing a CSV line into a Product object, returning an Optional to handle potential parsing errors. */
public class ProductParser {

    /* Parses a CSV line into a Product object, ensuring that the line has the correct number of fields and that numeric values are properly formatted. */
    public Optional<Product> parse(String line) {
        String[] fields = line.split(",");

        if (fields.length != 4) return Optional.empty();

        // Attempt to parse the product ID and price, returning an empty Optional if parsing fails or if required fields are empty.
        try {
            int id = Integer.parseInt(fields[0].trim());
            String name = fields[1];
            double price = Double.parseDouble(fields[2].trim());
            String category = fields[3];

            if (name.isEmpty() || category.isEmpty()) return Optional.empty();

            return Optional.of(new Product(id, name, price, category));

        } 
        // If parsing fails for product ID or price, return an empty Optional to indicate a skipped line.
        catch (NumberFormatException e) {
            return Optional.empty();
        }
    }
}