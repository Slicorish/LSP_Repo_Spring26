package org.howard.edu.lsp.assignment4;

public class CargoAircraft extends Aircraft {

    private double cargoWeight = 50000; // in pounds

    public CargoAircraft(String id,  String aircraft) {
        super(id, "Cargo"); 
    }

    @Override
    public void displayInfo() {
        System.out.println("Cargo Flight: " + flightID +
                           " Cargo Weight: " + cargoWeight);
    }
}