package org.howard.edu.lsp.finalexam.question2;


/**
 * Concrete {Report} that prints information about a single course.
 */
public class CourseReport extends Report {

    private String courseName;
    private int enrollment;

    /**
     * Loads the course-specific data used by the body of the report.
     */
    @Override
    protected void loadData() {
        this.courseName = "CSCI 363";
        this.enrollment = 45;
    }

    /**
     * @return the header text identifying this as a course report
     */
    @Override
    protected String formatHeader() {
        return "Course Report";
    }

    /**
     * @return the body text showing the course name and enrollment count
     */
    @Override
    protected String formatBody() {
        return "Course: " + courseName + "\n"
             + "Enrollment: " + enrollment;
    }

    /**
     * @return the footer text marking the end of the course report
     */
    @Override
    protected String formatFooter() {
        return "End of Course Report";
    }
}
