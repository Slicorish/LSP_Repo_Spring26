package org.howard.edu.lsp.midterm.strategy;
// HolidayPricingStrategy.java overrides the calculatePrice method to apply a 15% discount for holiday customers and overrides the calculatePrice method to apply a 15% discount for holiday customers.
public class HolidayPricingStrategy implements PricingStrategy {
    @Override
    public double calculatePrice(double price) {
        return price * 0.85;
    }
}
