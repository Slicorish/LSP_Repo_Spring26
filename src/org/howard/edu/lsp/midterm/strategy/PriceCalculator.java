package org.howard.edu.lsp.midterm.strategy;
import java.util.Map;

public class PriceCalculator {
// The PriceCalculator class contains a static map that associates customer types 
    private static final Map<String, PricingStrategy> strategies = Map.of(
        "REGULAR", new RegularPricingStrategy(),
        "MEMBER",  new MemberPricingStrategy(),
        "VIP",     new VIPPricingStrategy(),
        "HOLIDAY", new HolidayPricingStrategy()
    );

    // The calculatePrice method takes a customer type and a price, retrieves the appropriate pricing strategy from the map, and applies it to calculate the final price. If the customer type is not found in the map, it defaults to using the RegularPricingStrategy.
    public double calculatePrice(String customerType, double price) {
        PricingStrategy strategy = strategies.getOrDefault(customerType, new RegularPricingStrategy());
        return strategy.calculatePrice(price);
    }
}

