package org.howard.edu.lsp.assignment4;

import java.util.*;

public class AircraftDatabase {

    private Map<String, Aircraft> aircraftMap = new HashMap<>();

    public void addOrUpdateAircraft(Aircraft aircraft) {
        aircraftMap.put(aircraft.flightID, aircraft);
    }

    public Aircraft getAircraft(String flightID) {
        return aircraftMap.get(flightID);
    }

    public Collection<Aircraft> getAllAircraft() {
        return aircraftMap.values();
    }

    // Query by aircraft type
    public List<Aircraft> getAircraftByType(String type) {
        List<Aircraft> result = new ArrayList<>();
        for (Aircraft a : aircraftMap.values()) {
            if (a.aircraftType.equalsIgnoreCase(type)) {
                result.add(a);
            }
        }
        return result;
    }

    // Query by altitude range
    public List<Aircraft> getAircraftByAltitude(double minAlt, double maxAlt) {
        List<Aircraft> result = new ArrayList<>();
        for (Aircraft a : aircraftMap.values()) {
            if (a.altitude >= minAlt && a.altitude <= maxAlt) {
                result.add(a);
            }
        }
        return result;
    }

    // Query by partial flightID
    public List<Aircraft> searchByFlightID(String partialID) {
        List<Aircraft> result = new ArrayList<>();
        for (Aircraft a : aircraftMap.values()) {
            if (a.flightID.contains(partialID)) {
                result.add(a);
            }
        }
        return result;
    }
}
    
 
