package org.howard.edu.lsp.finalexam.question2;

import java.util.ArrayList;
import java.util.List;
 
/**
 * Driver that demonstrates polymorphic use of the  Report} hierarchy.
 *
 * <p>It stores {StudentReport} and {CourseReport} instances in a
 * {List<Report>} and invokes {generateReport()} on each.
 * The same call runs different concrete steps depending on the runtime
 * type of each report.
 */
public class Driver {
 
    public static void main(String[] args) {
        List<Report> reports = new ArrayList<>();
        reports.add(new StudentReport());
        reports.add(new CourseReport());
 
        for (Report report : reports) {
            report.generateReport();
        }
    }
}
 
