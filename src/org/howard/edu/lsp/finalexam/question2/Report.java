package org.howard.edu.lsp.finalexam.question2;

/**
 * Abstract base class that defines the Template Method for generating reports.
 *
 */
public abstract class Report {

    /**
     * Loads the data required by this report. Concrete subclasses must assign
     * any instance fields used by {formatBody()} here.
     */
    protected abstract void loadData();

    /**
     * Builds the report-specific header text (no banner line).
     * @return the header content
     */
    protected abstract String formatHeader();

    /**
     * Builds the report-specific body text.
     * @return the body content
     */
    protected abstract String formatBody();

    /**
     * Builds the report-specific footer text.
     * @return the footer content
     */
    protected abstract String formatFooter();

    /**
     * Template method that defines the fixed report-generation workflow.
     * This method is declared {@code final} so subclasses cannot change the
     * order of the steps.
     */
    public final void generateReport() {
        loadData();
        System.out.println("=== HEADER ===");
        System.out.println(formatHeader());
        System.out.println("=== BODY ===");
        System.out.println(formatBody());
        System.out.println("=== FOOTER ===");
        System.out.println(formatFooter());
    }
} 
    

