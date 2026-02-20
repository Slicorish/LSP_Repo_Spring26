package org.howard.edu.lsp.assignment3;

/* Product class represents a product with its attributes and manipulating its values through logic. */
public class Product {
    private int productId;
    private String name;
    private double price;
    private String category;

    /* Constructor initializes the product attributes and applies basic formatting to name and category. */
    public Product(int productId, String name, double price, String category) {
        this.productId = productId;
        this.name = name.trim().toUpperCase();
        this.price = price;
        this.category = category.trim();
    }
    /* Transforms the product by applying discounts and categorizing it. */
    public void transform() {
        applyDiscount();
        categorize();
    }
    
/* Applies a 10% discount to electronics products. */
    public void applyDiscount() {
        if (category.equals("Electronics")) {
            price *= 0.9;
        }
    }

    /* Categorizes products as "Premium Electronics" if they are electronics and priced above $500. */
    public void categorize() {
        if (price > 500 && category.equals("Electronics")) {
            category = "Premium Electronics";
        }
    }

    /* Determines the price range based on the price of the product. */
    public String getPriceRange() {
        if (price <= 10) return "Low";
        else if (price <= 100) return "Medium";
        else if (price <= 500) return "High";
        else return "Premium";
    }

    /* Converts the product's productID double value into a CSV format string to be written back into the file. */
    public String toCSV() {
        double rounded = Math.round(price * 100.0) / 100.0;
        return String.join(",",
            String.valueOf(productId),
            name,
            String.format("%.2f", rounded),
            category,
            getPriceRange()
        );
    }
}
