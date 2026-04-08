package org.howard.edu.lsp.assignment5;

import java.util.ArrayList;
import java.util.Collections;


public class IntegerSet {

    /** The internal list storing unique integer elements of this set. */
    private ArrayList<Integer> set = new ArrayList<>();

    /** Constructs an empty IntegerSet. */
    public IntegerSet(){

    }

    /**
     * Custom exception used for when an illegal operation is performed on an IntegerSet
     * 
     */
    public static class IntegerSetException extends RuntimeException {
        /**
         * Constructs an IntegerSetException with the given message.
         *
         * @param message description of the error
         */
        public IntegerSetException(String message) {
            super(message);
        }
    }
 
    /**
     * Constructs an IntegerSet initialized with the given list.
     *
     * @param set the initial list of integers (duplicates will be ignored on add)
     */
    public IntegerSet(ArrayList<Integer> set) {
        this.set = set;
    }

    /**
     * Removes all elements from this set, making it empty.
     */
    public void clear() {
        set.clear();
    }

    /**
     * Returns the number of elements in the set
     *
     * @return the size of the set
     */
    public int length() {
        return set.size();
    }

    /**
     * Returns true if the instantiated set and the given set contain exactly the same elements,
     * regardless of order.
     *
     * @param b the IntegerSet to compare against
     * @return true if both sets contain the same elements, false otherwise
     */
    public boolean equals(IntegerSet b) {
        if (this.length() != b.length()) return false;
        return this.set.containsAll(b.set);
    }

    /**
     * Returns true if the set contains the specified value in question.
     *
     * @param value the integer to search for
     * @return true if the value is in the set, false otherwise
     */
    public boolean contains(int value) {
        return set.contains(value);
    }

    /**
     * Returns the largest integer in the set.
     *
     * @return the largest element
     * @throws IntegerSetException if the set is empty
     */
    public int largest() throws IntegerSetException {
        if (isEmpty()) {
            throw new IntegerSetException("Set is empty");
        }
        return Collections.max(set);
    }

    /**
     * Returns the smallest integer in the set.
     *
     * @return the smallest element
     * @throws IntegerSetException if the set is empty
     */
    public int smallest() throws IntegerSetException {
        if (isEmpty()) {
            throw new IntegerSetException ("Set is empty");
        }
        return Collections.min(set);
    }

    /**
     * Adds the specified integer to the set if it is not already present.
     *
     * @param item the integer to add
     */
    public void add(int item) {
        if (!set.contains(item)) {
            set.add(item);
        }
    }

    /**
     * Removes the specified integer from the set if it is present.
     * Does nothing if the item is not in the set.
     *
     * @param item the integer to remove
     */
    public void remove(int item) {
        set.remove(Integer.valueOf(item));
    }

    /**
     * Returns a new IntegerSet containing all elements from the instantiated set and the given set.
     * Does not modify either original set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet representing the union
     */
    public IntegerSet union(IntegerSet intSetb) {
        IntegerSet resultingSet = new IntegerSet();
        resultingSet.set.addAll(this.set);
        for (int i : intSetb.set) {
            if (!resultingSet.set.contains(i)) {
                resultingSet.set.add(i);
            }
        }
        return resultingSet;
    }

    /**
     * Returns a new IntegerSet containing only the elements common to both sets.
     * Does not modify either original set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet representing the intersection
     */
    public IntegerSet intersect(IntegerSet intSetb) {
        IntegerSet resultingSet = new IntegerSet();
        for (int i : this.set) {
            if (intSetb.set.contains(i)) {
                resultingSet.set.add(i);
            }
        }
        return resultingSet;
    }

    /**
     * Returns a new IntegerSet containing elements in this set but not in the given set.
     * Does not modify either original set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet representing the difference (this - intSetb)
     */
    public IntegerSet diff(IntegerSet intSetb) {
        IntegerSet result = new IntegerSet();
        for (int item : this.set) {
            if (!intSetb.set.contains(item)) {
                result.set.add(item);
            }
        }
        return result;
    }

    /**
     * Returns a new IntegerSet containing elements in the given set but not in this set.
     * This is the complement of this set relative to intSetb.
     * Does not modify either original set.
     *
     * @param intSetb the other IntegerSet
     * @return a new IntegerSet representing the complement (intSetb - this)
     */
    public IntegerSet complement(IntegerSet intSetb) {
        IntegerSet resultingSet = new IntegerSet();
        for (int i : intSetb.set) {
            if (!this.set.contains(i)) {
                resultingSet.set.add(i);
            }
        }
        return resultingSet;
    }

    /**
     * Returns true if the set contains no elements.
     *
     * @return true if the set is empty, false otherwise
     */
    public boolean isEmpty() {
        return set.isEmpty();
    }

    /**
     * Returns a string representation of the set in ascending sorted order.
     * Format: [1, 2, 3] or [] for an empty set.
     *
     * @return string representation of the set
     */
    @Override
    public String toString() {
        ArrayList<Integer> sorted = new ArrayList<>(set);
        Collections.sort(sorted);
        return sorted.toString();
    }
}