package org.howard.edu.lsp.midterm.crccards;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TaskManager {
    private Map<String, Task> taskstoManage;

    public TaskManager() {
        this.taskstoManage = new HashMap<>();
    }

    public void addTask(Task task) {
        if (taskstoManage.containsKey(task.getTaskId())) {
            throw new IllegalArgumentException("Task with ID " + task.getTaskId() + " already exists.");
        }
        taskstoManage.put(task.getTaskId(), task);
    }

    public Task findTask(String taskId) {
        return taskstoManage.getOrDefault(taskId, null);
    }

    // I want to chand this part 

    public List<Task> getTasksByStatus(String status) {
        List<Task> result = new ArrayList<>();
        for (Task task : taskstoManage.values()) {
            if (task.getStatus().equals(status)) {
                result.add(task);
            }
        }
        return result;
    }
}