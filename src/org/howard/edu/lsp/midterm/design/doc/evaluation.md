Question: The following class is part of a simple order processing system. The design of this class violates several object-oriented design ideas discussed in class, including principles described in Arthur Riel’s object-oriented design heuristics. Study the code carefully and answer the questions that follow.

Answer: 
- the variables that represent the main attributes of the class are public, which means outside classes will have acces to them. That is poor encapsulation 
- the entire process is done within one method in an algorithmic structure, it actually lacks an object oriented structure altogether. This also violetates the single responsibility principle, because in this code their are multiple unrelated tasks occuring in the same class, including calculating tax, printing a receipt, saving the order to file, sending a confirmation email, and then applying a discount
- In addition, the discount is applied after the receipt is printed which is a flaw in logic