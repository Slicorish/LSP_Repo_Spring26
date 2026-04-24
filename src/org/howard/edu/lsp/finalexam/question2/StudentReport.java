package org.howard.edu.lsp.finalexam.question2;

/**
 * Concrete  Report} that prints information about a single student.
 */
public class StudentReport extends Report {

    private String studentName;
    private double gpa;

    /**
     * Loads the student-specific data used by the body of the report.
     */
    @Override
    protected void loadData() {
        this.studentName = "John Doe";
        this.gpa = 3.8;
    }

    /**
     * @return the header text identifying this as a student report
     */
    @Override
    protected String formatHeader() {
        return "Student Report";
    }

    /**
     * @return the body text showing the student's name and GPA
     */
    @Override
    protected String formatBody() {
        return "Student Name: " + studentName + "\n"
             + "GPA: " + gpa;
    }

    /**
     * @return the footer text marking the end of the student report
     */
    @Override
    protected String formatFooter() {
        return "End of Student Report";
    }
}
