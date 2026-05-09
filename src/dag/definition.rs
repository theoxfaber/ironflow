use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use chrono::{DateTime, Utc};

/// Represents a complete DAG (Directed Acyclic Graph)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagDefinition {
    pub id: String,
    pub description: Option<String>,
    pub schedule: Option<String>, // cron expression
    pub max_active_runs: Option<u32>,
    pub catchup: Option<bool>,
    pub tasks: Vec<TaskDefinition>,
}

/// Represents a single task within a DAG
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskDefinition {
    pub id: String,
    pub operator: String,
    pub depends_on: Option<Vec<String>>,
    pub retries: Option<u32>,
    pub retry_delay_secs: Option<u64>,
    pub timeout_secs: Option<u64>,
    pub xcom_inputs: Option<Vec<String>>, // Task IDs whose outputs to inject
    #[serde(flatten)]
    pub config: serde_json::Value, // Operator-specific config
}

impl TaskDefinition {
    pub fn dependencies(&self) -> Vec<String> {
        self.depends_on.clone().unwrap_or_default()
    }

    pub fn xcom_dependencies(&self) -> Vec<String> {
        self.xcom_inputs.clone().unwrap_or_default()
    }
}

/// Represents a DAG run (one execution instance)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagRun {
    pub id: String,
    pub dag_id: String,
    pub status: DagRunStatus,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
    pub triggered_by: TriggerType,
    pub run_number: u32,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DagRunStatus {
    Queued,
    Running,
    Success,
    Failed,
}

impl std::fmt::Display for DagRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DagRunStatus::Queued => write!(f, "queued"),
            DagRunStatus::Running => write!(f, "running"),
            DagRunStatus::Success => write!(f, "success"),
            DagRunStatus::Failed => write!(f, "failed"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TriggerType {
    Schedule,
    Manual,
}

impl std::fmt::Display for TriggerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TriggerType::Schedule => write!(f, "schedule"),
            TriggerType::Manual => write!(f, "manual"),
        }
    }
}

/// Represents a task run (one task execution in a DAG run)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRun {
    pub id: String,
    pub dag_run_id: String,
    pub task_id: String,
    pub status: TaskRunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub attempt_number: u32,
    pub log: String,
    pub xcom_output: Option<String>, // JSON
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TaskRunStatus {
    Pending,
    Running,
    Success,
    Failed,
    Retried,
    Skipped,
}

impl std::fmt::Display for TaskRunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskRunStatus::Pending => write!(f, "pending"),
            TaskRunStatus::Running => write!(f, "running"),
            TaskRunStatus::Success => write!(f, "success"),
            TaskRunStatus::Failed => write!(f, "failed"),
            TaskRunStatus::Retried => write!(f, "retried"),
            TaskRunStatus::Skipped => write!(f, "skipped"),
        }
    }
}

impl DagDefinition {
    /// Get all task IDs in topological order for execution
    pub fn task_execution_order(&self) -> Result<Vec<String>, String> {
        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut visiting = HashSet::new();

        for task in &self.tasks {
            self.dfs(&task.id, &mut result, &mut visited, &mut visiting)?;
        }

        Ok(result)
    }

    fn dfs(
        &self,
        task_id: &str,
        result: &mut Vec<String>,
        visited: &mut HashSet<String>,
        visiting: &mut HashSet<String>,
    ) -> Result<(), String> {
        if visited.contains(task_id) {
            return Ok(());
        }

        if visiting.contains(task_id) {
            return Err(format!("Cycle detected involving task: {}", task_id));
        }

        visiting.insert(task_id.to_string());

        if let Some(task) = self.tasks.iter().find(|t| t.id == task_id) {
            for dep in &task.dependencies() {
                self.dfs(dep, result, visited, visiting)?;
            }
        }

        visiting.remove(task_id);
        visited.insert(task_id.to_string());
        result.push(task_id.to_string());

        Ok(())
    }

    /// Get tasks that have no dependencies (can run immediately)
    pub fn root_tasks(&self) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|t| t.dependencies().is_empty())
            .map(|t| t.id.clone())
            .collect()
    }

    /// Get tasks that depend on a given task
    pub fn dependents(&self, task_id: &str) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|t| t.dependencies().contains(&task_id.to_string()))
            .map(|t| t.id.clone())
            .collect()
    }

    /// Get a task by ID
    pub fn get_task(&self, task_id: &str) -> Option<&TaskDefinition> {
        self.tasks.iter().find(|t| t.id == task_id)
    }

    /// Check if all dependencies of a task are satisfied
    pub fn dependencies_satisfied(
        &self,
        task_id: &str,
        completed_tasks: &HashSet<String>,
    ) -> bool {
        if let Some(task) = self.get_task(task_id) {
            task.dependencies()
                .iter()
                .all(|dep| completed_tasks.contains(dep))
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_execution_order() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                TaskDefinition {
                    id: "a".to_string(),
                    operator: "bash".to_string(),
                    depends_on: None,
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
                TaskDefinition {
                    id: "b".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["a".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
                TaskDefinition {
                    id: "c".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["b".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
            ],
        };

        let order = dag.task_execution_order().unwrap();
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_cycle_detection() {
        let dag = DagDefinition {
            id: "cyclic_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                TaskDefinition {
                    id: "a".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["c".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
                TaskDefinition {
                    id: "b".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["a".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
                TaskDefinition {
                    id: "c".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["b".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
            ],
        };

        let result = dag.task_execution_order();
        assert!(result.is_err());
    }

    #[test]
    fn test_root_tasks() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                TaskDefinition {
                    id: "a".to_string(),
                    operator: "bash".to_string(),
                    depends_on: None,
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
                TaskDefinition {
                    id: "b".to_string(),
                    operator: "bash".to_string(),
                    depends_on: Some(vec!["a".to_string()]),
                    retries: None,
                    retry_delay_secs: None,
                    timeout_secs: None,
                    xcom_inputs: None,
                    config: serde_json::json!({}),
                },
            ],
        };

        let roots = dag.root_tasks();
        assert_eq!(roots, vec!["a"]);
    }
}
