package org.howard.edu.lsp.midterm.strategy;

public class RegularPricingStrategy implements PricingStrategy {
   // RegularPricingStrategy.java implements the calculatePrice method to return the original price without any discount and overrides the calculatePrice method to return the original price without any discount.
    @Override
    public double calculatePrice(double price) {
        return price;
    }
}