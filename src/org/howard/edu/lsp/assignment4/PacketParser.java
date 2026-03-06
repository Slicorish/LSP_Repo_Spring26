package org.howard.edu.lsp.assignment4;

public class PacketParser {

    public Aircraft parsePacket(TransponderPacket packet) {

        // Example pseudo parsing
        String[] data = packet.getRawData().split(",");

        String type = data[0];
        String flightID = data[1];

        if(type.equals("Passenger"))
            return new CommercialAircraft(flightID, "Boeing 737");

        else if(type.equals("Cargo"))
            return new CargoAircraft(flightID, "Freighter");

        return null;
    }
}