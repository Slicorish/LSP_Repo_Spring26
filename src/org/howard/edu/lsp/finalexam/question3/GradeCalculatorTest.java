package org.howard.edu.lsp.finalexam.question3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * JUnit 5 tests for {GradeCalculator}.
 *
 * Covers a normal case for every public method, two boundary-value cases
 * around the passing/letter-grade cutoffs, and two exception cases that verify
 * {IllegalArgumentException} is thrown for out-of-range scores.
 */
public class GradeCalculatorTest {

    private GradeCalculator calc;

    @BeforeEach
    void setUp() {
        calc = new GradeCalculator();
    }

    
    @Test
    void averageTest() {
        assertEquals(85.0, calc.average(80, 85, 90), 1e-9);
    }

    @Test
    void letterGradeTest() {
        assertEquals("B", calc.letterGrade(85.0));
    }

  
    @Test
    void isPassingTest() {
        assertTrue(calc.isPassing(75.0));
    }

    @Test
    void isPassingBoundaryTest() {
        assertTrue(calc.isPassing(60.0));
    }

    @Test
    void belowPassingBoundaryTest() {
        assertFalse(calc.isPassing(59.9));
        assertEquals("F", calc.letterGrade(59.9));
    }


    @Test
    void NegativeScoreExceptionTest() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> calc.average(-1, 80, 90));
        assertEquals("Score must be between 0 and 100", ex.getMessage());
    }

    @Test
    void AboveUpperBoundExceptionTest() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> calc.average(80, 90, 101));
        assertEquals("Score must be between 0 and 100", ex.getMessage());
    }
}
