package org.howard.edu.lsp.assignment4;

public class ATCReceiver {
    public static void main(String[] args) {
        // Create the aircraft database
        AircraftDatabase database = new AircraftDatabase();
        // Create the packet parser
        PacketParser parser = new PacketParser();

        // Simulate receiving packets from arriving aircraft
        String[] packets = {
            "Passenger,AA123,38.9,-77.0,30000,500",
            "Cargo,BB456,39.0,-77.1,28000,450"
        };

        for (String raw : packets) {
            TransponderPacket packet = new TransponderPacket(raw);
            Aircraft aircraft = parser.parsePacket(packet);
            if (aircraft != null) 
                database.addOrUpdateAircraft(aircraft);
            }
        

        // Display the database contents
        DisplaySystem display = new DisplaySystem();
        display.updateDisplay(database);
    }
}

