package org.howard.edu.lsp.assignment6;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;

public class IntegerSetTest {

     private IntegerSet set1;
    private IntegerSet set2;

   
    @BeforeEach
    public void setUp() {
        set1 = new IntegerSet();
        set2 = new IntegerSet();

        set1.add(10);
        set1.add(20);
        set1.add(30);

        set2.add(20);
        set2.add(30);
        set2.add(40);
    }

    @Test
    @DisplayName("Clear() test cases: 1 normal and 1 edge case")
    public void testClear() {
        set1.clear(); //set 1 = []
        assertEquals("[]", set1.toString(), "After clear(), set should be empty");

        set1.clear(); // clearing again should not throw an error on an already empty set
        assertEquals("[]", set1.toString(), "Clearing an already empty set should still be empty");
    }

    @Test
    @DisplayName("Length() test cases: 1 normal and 2 edge cases")
    public void testLength() {
        assertEquals(3, set1.length(), "Length should be 3"); //set 1 = [10, 20, 30]

        set1.remove(20); //set 1 = [10, 30]
        assertEquals(2, set1.length(), "Length should decrease to 2 after removing an element");

        set1.clear(); //set 1 = []
        assertEquals(0, set1.length(), "Length of an empty set should be 0");
    }



    @Test
    @DisplayName("Equals() test cases: 1 normal and 3 edge cases")
    public void testEquals() {
        IntegerSet a = new IntegerSet();
        a.add(10); a.add(20); a.add(30); // a = [10, 20, 30] and set1 = [10, 20, 30]
        assertTrue(set1.equals(a), "Two identical sets with the same elements should be equal");

        //set 2 = [20, 30, 40]
        assertFalse(set1.equals(set2), "Two sets with different elements should not be equal");

        set2.remove(40); //set 2 = [20, 30]
        set2.add(10); //set 2 = [10, 20, 30]
        assertTrue(set1.equals(set2), "Sets with the same elements should be equal");

        set1.clear(); //set 1 = []
        assertFalse(set1.equals(set2), "An empty set should not be equal to a non-empty set");
    }

        @Test
    @DisplayName("Contains() test cases: 1 normal and 2 edge cases")
    public void testContains() {
        assertTrue(set1.contains(10), "Set should contain 10"); // set 1 = [10, 20, 30]

        assertFalse(set1.contains(99), "Set should not contain 99");

        set2.clear(); //set 2 = []
        assertFalse(set2.contains(40), "Empty set should not contain 40");
    }


@Test
    @DisplayName("Largest() test cases: 1 normal and 3 edge cases")
    public void testLargestNormal() {
        assertEquals(30, set1.largest(), "Largest should be 30"); // set 1 = [10, 20, 30]

        set2.remove(40); //set 2 = [20, 30]
        set2.remove(30); //set 2 = [20]
        assertEquals(20, set2.largest(), "Largest should be 20 after removing 30 and 40 and being a single element set");

        set1.add(50); //set 1 = [10, 20, 30, 50]
        assertEquals(50, set1.largest(), "Largest should be 50 after adding it to the set");

        set1.clear(); //set 1 = []
        assertThrows(IntegerSet.IntegerSetException.class, set1::largest,
                "largest() on empty set should throw IntegerSetException");
    }


    @Test
    @DisplayName("Smallest() test cases: 1 normal and 2 edge cases")
    public void testSmallest() {
        assertEquals(10, set1.smallest(), "Smallest should be 10"); // set 1 = [10, 20, 30]

        set1.remove(20); //set 1 = [10, 30]
        set1.remove(10); //set 1 = [30]
        assertEquals(30, set1.smallest(), "Smallest should be 30 after removing 10 and 20, leaving a single element set");

        set1.clear(); //set 1 = []
        assertThrows(IntegerSet.IntegerSetException.class, set1::smallest,
                "smallest() on empty set should throw IntegerSetException");
    }

   
    @Test
    @DisplayName("Add() test cases: 1 normal and 2 edge cases")
    public void testAdd() {
        set1.add(-20); //set 1 stated at [10, 20, 30], now = [10, 20, 30, -20]
        assertTrue(set1.contains(-20), "Set should contain -20 after adding to an already initialized set");
        System.out.println("Set 1 after adding -20: " + set1.toString() + " with a length of " + set1.length());

        set2.clear(); //set 2 = []
        set2.add(50); //set 2 = [50]
        assertTrue(set2.contains(50), "Set should contain 50 after clearing set 2 and adding 50");

        set1.add(10); // adding a duplicate value; set 1 should still be [10, 20, 30, -20] 
        assertEquals(4, set1.length(), "Adding a duplicate value should not increase length of the set");
        System.out.println("Set 1 after adding duplicate 10: " + set1.toString() + " with a length of " + set1.length());
    }

    
    @Test
    @DisplayName("Remove() test cases: 1 normal and 2 edge cases")
    public void testRemovel() {
        set1.remove(30); //set 1 started at [10, 20, 30], after removal  = [10, 20]
        assertFalse(set1.contains(30), "30 should no longer be in the set");
        assertEquals(2, set1.length(), "Length should decrease to 2 after removing an element");


        set1.remove(0); //set 1 = [10, 20]
        assertEquals(2, set1.length(), "Removing a non-existent element should not change the set or its length");

        set1.clear(); //set 1 = []
        set1.remove(10); // removing from an empty set should not throw an error
        assertEquals(0, set1.length(), "Length should remain 0 after trying to remove from an empty set");
    }
  

    @Test
    @DisplayName("Union test cases: 1 normal and 2 edge cases")
    public void testUnion() {
        System.out.println("Set 1: " + set1.toString()); //set1 = [10, 20, 30]
        IntegerSet a = new IntegerSet(); //set a = []
        IntegerSet result = set1.union(a);
        assertEquals(3, result.length(), "Union with an empty set should equal the original set");
        assertTrue(result.contains(10) && result.contains(20) && result.contains(30), "Union with empty set should contain all original elements");
       
        a.add(40); // set a = [40]
        a.add(50); // set a = [40, 50]
        a.add(20); // duplicate element to test that it doesn't get added twice
        result = set1.union(a);
        assertEquals(5, result.length(), "Union should contain all unique elements from both sets");

        set2.remove(40); //set 2 = [20, 30]
        set2.add(10); //set2 = [10, 20, 30]
        result = set1.union(set2);
        assertEquals(3, result.length(), "Union of sets with the same elements should not have duplicates");
    }

    @Test
    @DisplayName("Intersect() test cases: 1 normal 2 edge case")
    public void testIntersect() {
        IntegerSet result = set1.intersect(set2); //set1 = [10, 20, 30], set2 = [20, 30, 40]
        assertEquals(2, result.length(), "Intersection should have 2 elements");

        set2.remove(20); //set 2 = [30, 40]
        set2.remove(30); //set 2 = [40]
        result = set1.intersect(set2);
        assertTrue(result.isEmpty(), "Intersection of sets with no common elements should be empty");

        set2.clear(); //set 2 = []
        result = set1.intersect(set2);
        assertTrue(result.isEmpty(), "Intersection with an empty set should be empty");
       
    }



    @Test
    @DisplayName("Test diff() returns elements in this but not in other")
    public void testDiff() {
        set2.remove(30); //set 2 = [20, 40]; set 1 = [10, 20, 30]
        IntegerSet result = set1.diff(set2);
        assertEquals(2, result.length(), "Diff should contain 2 elements");
        assertTrue(result.contains(10) && result.contains(30), "Diff should contain 10 and 30");
        
        set2.remove(40); //set 2 = [20]
        set2.add(10); //set 2 = [10, 20]
        set1.remove(30); //set 1 = [10, 20]
        result = set1.diff(set2);
        assertTrue(result.isEmpty(), "Diff should be empty whith identical sets");

        set2.clear(); //set 2 = []
        result = set1.diff(set2);
        assertEquals(2, result.length(), "Diff with an empty set should return all elements");
    }

   

    @Test
    @DisplayName("Complement() test cases: 1 normal and 2 edge cases")
    public void testComplement() {
       set1.remove(30); //set 1 = [10, 20]; set 2 = [20, 30, 40]
        IntegerSet result = set1.complement(set2);
        assertEquals(2, result.length(), "Complement should contain elements in set2 not in set1");
        assertTrue(result.contains(30) && result.contains(40));
        assertFalse(result.contains(20), "Complement should not contain elements that are in set1");

        set2.remove(20); //set 2 = [30, 40]; set 1 = [10, 20]
        result = set1.complement(set2);
        assertEquals(2, result.length(), "Complement should still contain 2 elements after removing 20 from set2");
        assertTrue(result.contains(30) && result.contains(40), "Complement of disjoint sets should contain all elements of set2");

        set2.clear(); //set 2 = []
        result = set1.complement(set2);
        assertTrue(result.isEmpty(), "Complement with an empty set should be empty");
    }



    @Test
    @DisplayName("isEmpty() test cases: 1 normal and 2 edge cases")
    public void testIsEmpty() {
        assertFalse(set1.isEmpty(), "Non-empty set should not be empty"); //set 1 = [10, 20, 30]

        IntegerSet newSet = new IntegerSet(); //newSet = []
        assertTrue(newSet.isEmpty(), "Newly constructed set should be empty");

       set1.clear(); //set 1 = []
        assertTrue(set1.isEmpty(), "Set should be empty after clear()");
    }

  
    @Test
    @DisplayName("toString() test cases: 1 normal and 2 edge cases")
    public void testToString() {
       set1.add(25); //set 1 = [10, 20, 25, 30]
        assertEquals("[10, 20, 25, 30]", set1.toString(), "toString() should return all elements but in ascending order");

        set2.clear(); //set 2 = []
        assertEquals("[]", set2.toString(), "toString() of empty set should be []");

        set2.add(40); //set 2 = [40]
        assertEquals("[40]", set2.toString(), "toString() of single-element set should be [40]");
    }
}