package org.howard.edu.lsp.midterm.strategy;
// VIPPricingStrategy.java overrides the calculatePrice method to apply a 20% discount for VIP customers.
public class VIPPricingStrategy implements PricingStrategy {
    @Override
    public double calculatePrice(double price) {
        return price * 0.80;
    }
}
