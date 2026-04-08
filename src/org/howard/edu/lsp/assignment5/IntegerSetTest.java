package org.howard.edu.lsp.assignment5;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class IntegerSetTest {
    private IntegerSet set1;
    private IntegerSet set2;

    @BeforeEach
    public void setUp() {
        set1 = new IntegerSet();
        set2 = new IntegerSet();

        set1.add(1);
        set1.add(2);
        set1.add(3);

        set2.add(2);
        set2.add(3);
        set2.add(4);
    }

    @Test
    @DisplayName("Testing the clear method")
    public void testClear() {
        set1.clear();
        assertEquals("[]", set1.toString());
    }

    @Test
    @DisplayName("Testing the length method")
    public void testLength() {
        assertEquals(3, set1.length());
        set1.remove(2);
        assertEquals(2, set1.length());
    }

    @Test
    @DisplayName("Testing the equals method")
    public void testEquals() {
        IntegerSet set3 = new IntegerSet();
        set3.add(3);
        set3.add(2);
        set3.add(1);
        assertTrue(set1.equals(set3));
        assertFalse(set1.equals(set2));
    }

    @Test
    @DisplayName("Testing the contains method")
    public void testContains() {
        assertTrue(set1.contains(2));
    }

    @Test
    @DisplayName("Testing the largest method")
    public void testLargest() {
        assertEquals(3, set1.largest());
    }

    @Test
    @DisplayName("Testing the smallest method")
    public void testSmallest() {
        assertEquals(1, set1.smallest());
    }

    @Test
    @DisplayName("Testing the add method")
    public void testAdd() {
        IntegerSet set = new IntegerSet();
        set.add(1);
        set.add(2);
        set.add(3);
        assertEquals("[1, 2, 3]", set.toString());
        set.add(1);
        assertEquals("[1, 2, 3]", set.toString());
    }

    @Test
    @DisplayName("Testing the remove method")
    public void testRemove() {
        set1.remove(2);
        assertEquals("[1, 3]", set1.toString());
    }

    @Test
    @DisplayName("Testing the union method")
    public void testUnion() {
        IntegerSet result = set1.union(set2);
        assertEquals("[1, 2, 3, 4]", result.toString());
    }

    @Test
    @DisplayName("Testing the intersect method")
    public void testIntersect() {
        IntegerSet result = set1.intersect(set2);
        assertEquals("[2, 3]", result.toString());
    }

    @Test
    @DisplayName("Testing the diff method")
    public void testDiff() {
        IntegerSet result = set1.diff(set2);
        assertEquals("[1]", result.toString());
    }

    @Test
    @DisplayName("Testing the complement method")
    public void testComplement() {
        IntegerSet result = set1.complement(set2);
        assertEquals("[4]", result.toString());
    }

    @Test
    @DisplayName("Testing the isEmpty method")
    public void testIsEmpty() {
        assertFalse(set1.isEmpty());
        set1.clear();
        assertTrue(set1.isEmpty());
    }

    @Test
    @DisplayName("Testing the toString method")
    public void testToString() {
        assertEquals("[1, 2, 3]", set1.toString());
        assertEquals("[2, 3, 4]", set2.toString());
    }
}
