package org.howard.edu.lsp.assignment4;

public class CommercialAircraft extends Aircraft {
    private int passengerCapacity = 200;
    public CommercialAircraft(String ID, String aircraft) {
        super(ID, aircraft);
    }

    @Override
    public void displayInfo() {
        System.out.println("Passenger Flight: " + flightID +
                           " Capacity: " + passengerCapacity);
    }
    
}

