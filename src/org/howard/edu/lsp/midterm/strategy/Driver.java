package org.howard.edu.lsp.midterm.strategy;

public class Driver {
    public static void main(String[] args) {
        double price = 100.0;

        PricingStrategy regular = new RegularPricingStrategy();
        PricingStrategy member = new MemberPricingStrategy();
        PricingStrategy vip = new VIPPricingStrategy();
        PricingStrategy holiday = new HolidayPricingStrategy();

        System.out.println("REGULAR: " + regular.calculatePrice(price));
        System.out.println("MEMBER: " + member.calculatePrice(price));
        System.out.println("VIP: " + vip.calculatePrice(price));
        System.out.println("HOLIDAY: " + holiday.calculatePrice(price));
    }
}
