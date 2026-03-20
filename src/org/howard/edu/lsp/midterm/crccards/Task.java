package org.howard.edu.lsp.midterm.crccards;

public class Task {
    private String taskId;
    private String description;
    private String status;
// The Task constructor initializes the task with a unique identifier, a description, and sets the default status to "OPEN". It takes two parameters: taskId and description, which are assigned to the corresponding instance variables. The status is initialized to "OPEN" by default.
    public Task(String taskId, String description) {
        this.taskId = taskId;
        this.description = description;
        this.status = "OPEN"; // Default status
    }
//  The getTaskId method returns the unique identifier of the task.
    public String getTaskId() {
        return taskId;
    }
// The getDescription method returns the description of the task.
    public String getDescription() {
        return description;
    }
// The setStatus method updates the status of the task. It checks if the provided status is one of the valid options ("OPEN", "IN_PROGRESS", "COMPLETE") and sets it accordingly. If an invalid status is provided, it defaults to "UNKNOWN".
   public void setStatus(String status){
    if (status.equals("OPEN") || status.equals("IN_PROGRESS") || status.equals("COMPLETE")) {
        this.status = status;
    } else {
        this.status = "UNKNOWN";
    }
}
    // The getStatus method returns the current status of the task.
    public String getStatus() {
        return status;  
}
    

    // The toString method provides a string representation of the task, including its ID, description, and status in the desired output
    @Override
    public String toString() {
        return taskId + " " + description + " [" + status + "]";
    }
}
