Class: AircraftDatabase

Responsibilities:

Store and manage all aircraft objects currently tracked by the ATC system.
Add new aircraft or update existing aircraft data using the aircraft’s flight ID.
Retrieve aircraft information by flight ID.
Provide lists of aircraft based on search queries (type, altitude range, or partial flight ID).
Provide a collection of all aircraft for display and system monitoring.

Collaborators (if any):

Aircraft
DisplaySystem

Assumptions (if any):

Each aircraft has a unique flightID.
Aircraft objects contain attributes such as aircraft type, altitude, and location.
The database is small enough to be stored in memory using a HashMap.



Class: CargoAircraft

Responsibilities:

Represent a cargo aircraft in the ATC system.
Store cargo-specific information such as cargo weight.
Inherit common aircraft properties (flight ID, altitude, location, etc.) from the Aircraft superclass.
Override the display behavior to show cargo-specific flight details.

Collaborators (if any):

Aircraft (superclass)
DisplaySystem (which calls displayInfo())

Assumptions (if any):

Cargo aircraft always have a default cargo weight unless updated.
The aircraft type is automatically set to "Cargo" when constructed.



Class: CommercialAircraft

Responsibilities:

Represent a commercial aircraft in the ATC system.
Store commercial- aircraft specific information such as passenger capacity.
Inherit common aircraft properties (flight ID, altitude, location, etc.) from the Aircraft superclass.
Override the display behavior to show commercial-specific flight details.

Collaborators (if any):

Aircraft (superclass)
DisplaySystem (which calls displayInfo())

Assumptions (if any):

Commercial aircrafts always have a default passenger capacity unless updated.
The aircraft type is automatically set to "Passanger Flight" when constructed.



Class: DisplaySystem

Responsibilities:

Periodically display aircraft information from the database.
Update the system display with current aircraft data.
Detect potentially dangerous situations (possible collision risks).
Calculate distance between aircraft based on latitude and longitude.
Allow air traffic controllers to query aircraft details by flight ID.
The display updates periodically every 10 seconds 


Collaborators (if any):

AircraftDatabase
Aircraft


Assumptions (if any):

Aircraft provide location data (latitude, longitude, altitude).
Aircraft implement displayInfo() and getDetails() methods.
A collision risk occurs if aircraft are within 1 mile and within 1000 ft altitude difference.



Class: Aircraft (Superclass)

Responsibilities:

Represent a general aircraft tracked by the ATC system.
Store common aircraft data such as: flightID, aircraftType, latitude, longitude, altitude
Provide methods for displaying aircraft information.
Provide detailed aircraft information when queried.

Collaborators (if any):

AircraftDatabase
DisplaySystem
CargoAircraft (subclass)
Commercial Aircraft (subclass)

Assumptions (if any):
All aircraft types inherit from this base class.
Subclasses override displayInfo() to customize how aircraft information is displayed.



Class: Packet Parser

Responsibilities:

Functions as a utility class
Interprets raw CSV-style strings from transponder packets.
Determines which subclass of Aircraft (Commercial or Cargo) to instantiate based on packet data.

Collaborators:

TransponderPacket
CommercialAircraft 
CargoAircraft 
Aircraft (super class)

Assumptions (if any): 

None



Class: Transponder Packet

Responsibilities:

Acts as a simple data wrapper (DTO) for raw incoming signal strings.

Collaborators:

Packet Parser

Assumptions (if any): 

Further implementation would require more attributes



Class: ATCReceiver

Responsibilities:

Simulates the arrival of raw transponder data packets.
Orchestrates the flow between parsing data and updating the database.
Initializes the display system to show the current airspace status.

Collaborators:

AircraftDatabase
PacketParser 
TransponderPacket
DisplaySystem 
Aircraft *and any implemented subclasses

Assumptions (if any): 

It assumes the format of the transponder packet string is consistent and correct 
It assumes PacketParser can successfully parse all valid packets and create appropriate Aircraft objects.
It assumes AircraftDatabase, PacketParser, and DisplaySystem are available and function as expected.
It assumes all required data (like position and speed) is present in each packet.

