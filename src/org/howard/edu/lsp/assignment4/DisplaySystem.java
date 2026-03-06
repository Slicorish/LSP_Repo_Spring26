package org.howard.edu.lsp.assignment4;

public class DisplaySystem {

    // Periodically update display every 10 seconds, for 3 updates, for the sake of demonstration
    public void startDisplay(AircraftDatabase database) {
        for (int i = 0; i < 3; i++) { 
            updateDisplay(database);
            try {
                Thread.sleep(10000); // 10 seconds between updates
            } catch (InterruptedException e) {
                // Ignore
            }
        }
    }

    public void updateDisplay(AircraftDatabase database) {
        System.out.println("---- Aircraft Display ----");
        for (Aircraft aircraft : database.getAllAircraft()) {
            aircraft.displayInfo();
        }
        // Dangerous situation detection
        detectDangerousSituations(database);
    }

    // Detect aircraft within 1 mile and 1000 ft altitude
    private void detectDangerousSituations(AircraftDatabase database) {
        Aircraft[] aircraftArray = database.getAllAircraft().toArray(new Aircraft[0]);
        for (int i = 0; i < aircraftArray.length; i++) {
            for (int j = i + 1; j < aircraftArray.length; j++) {
                double distance = calculateDistance(aircraftArray[i], aircraftArray[j]);
                double altDiff = Math.abs(aircraftArray[i].altitude - aircraftArray[j].altitude);
                if (distance < 1.0 && altDiff < 1000) {
                    System.out.println("WARNING: Possible collision risk between "
                        + aircraftArray[i].flightID + " and " + aircraftArray[j].flightID);
                }
            }
        }
    }

    private double calculateDistance(Aircraft a1, Aircraft a2) {
        // Simple calculation for demo purposes)
        double latDiff = a1.latitude - a2.latitude;
        double lonDiff = a1.longitude - a2.longitude;
        return Math.sqrt(latDiff * latDiff + lonDiff * lonDiff);
    }

    // Controller query functionality
    public void queryAircraft(AircraftDatabase database, String flightID) {
        Aircraft aircraft = database.getAircraft(flightID);
        if (aircraft != null) {
            System.out.println("Details for " + flightID + ": " + aircraft.getDetails());
        } else {
            System.out.println("Aircraft " + flightID + " not found.");
        }
    }
}