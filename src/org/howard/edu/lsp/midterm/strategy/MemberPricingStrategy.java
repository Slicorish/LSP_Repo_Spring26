package org.howard.edu.lsp.midterm.strategy;

public class MemberPricingStrategy implements PricingStrategy {
    // MemberPricingStrategy.java implements the calculatePrice method to apply a 10% discount for member customers and overrides the calculatePrice method to apply a 10% discount for member customers.
    @Override
    public double calculatePrice(double price) {
        return price * 0.90;
    }
}
