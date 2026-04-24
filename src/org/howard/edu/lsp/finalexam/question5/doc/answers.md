Identify three heuristics discussed in lecture and, for each:
1.	State the heuristic
2.	Briefly explain how it improves readability and/or maintainability.  Describe how the heuristic was explained or illustrated in lecture.  So, your answer must be constrained to how we discussed the heuristic in class.

Heuristic 1:
Name: No God Classes
Explanation: A “God class” tries to do everything in the system. By not creating "God Classes", smaller, focused classes are easier to understand and the code mirrors real-world concepts. In addition, changes won’t risk breaking unrelated features. This also makes debugging easier because issues are localized to specific classes. This concept was illustrated in lecture while we were covering object-oriented design and it was a crucial part of our homework assignment #4 which was based on CRC cards. 

Heuristic #2:
Name:  Don’t Turn an Operation into a Class (Avoid Verb-Based, Single-Action Classes)
Explanation: Noun-based classes make the system as a whole easier to model and makes it clearer where behavior logically belongs. This also prevents unnecessary fragmentation of logic across too many classes. This concept was also a major component of us covering object-oriented design and our homework assignment 4, which was based on CRC Cards. 


Heuristic #3: 
Name: Base Classes Should Not Know About Derived Classes
Explanation: This keeps class hierarchies clean and predictable. This also allows developers to understand a base class without needing to know all its subclasses. In terms of maintainability, this means you can add or modify subclasses without changing the base class. This prevents tight coupling and makes systems for scalable over time. This was a crucial part of our lecture topics of inheritance and frameworks. 