package org.howard.edu.lsp.assignment4;

// Abstract base class demonstrating Inheritance
abstract class Aircraft {
    protected String flightID;
    protected String aircraftType;
    protected double latitude;
    protected double longitude;
    protected double altitude;
    protected double speed;

    public Aircraft(String ID, String aircraft) {
        this.flightID = ID; 
        this.aircraftType = aircraft;
       
    }

    public void updatePosition(double lat, double lon, double alt, double speed) {
        this.latitude = lat;
        this.longitude = lon;
        this.altitude = alt;
        this.speed = speed;
    }

    public String getDetails() {
        return String.format("[%s] Lat: %.2f, Lon: %.2f, Alt: %.0fft", latitude, longitude, altitude);
    }
    public abstract void displayInfo();
}

    

