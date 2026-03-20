Question: Redesign the system to improve its object-oriented structure. Express your proposed design using CRC cards (Class–Responsibility–Collaborator). Your CRC cards should identify the major components of the redesigned system and how responsibilities are distributed among them.

Answer: My suggestion when it comes to imporoving the object-oriented structure of the OrderProcessor Class would be to break down the original class into smaller, easier to manage classes. The classes would be called the following: Customer, Order, OrderDatabase, EmailService, and Activity Tracker  

Class #1 ~ Customer: 
Responsibilities - Store customer name, Store email address
Collaborators -Order, EmailService

Class #2 ~ Order: 
Responsibilities - Hold item name and price, Calculate tax, Apply discount rules, Return order total, print receipt
Collaborators - Customer, OrderDatabase

Class #3 ~ OrderDatabase
Responsibilities - Save order to file, Handle file input and output errors, Format order record
Collaborators- Order, Customer

Class #4 ~ Email Service 
Responsibilities - Send confirmation email, Format email message, Handle send failures
Collaborators - Customer, Order

Class # 5 ~ Activity Tracker
Responsibilities - Record timestamped events, Write to the console to log output, format log messages
Collaborators - None
