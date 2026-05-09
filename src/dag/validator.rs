use super::definition::DagDefinition;
use anyhow::{anyhow, Result};

pub struct DagValidator;

impl DagValidator {
    /// Validate a DAG definition
    pub fn validate(dag: &DagDefinition) -> Result<()> {
        // Check DAG ID is not empty
        if dag.id.is_empty() {
            return Err(anyhow!("DAG ID cannot be empty"));
        }

        // Check for no cycles
        dag.task_execution_order()
            .map_err(|e| anyhow!("DAG validation failed: {}", e))?;

        // Check all tasks have unique IDs
        let mut task_ids = std::collections::HashSet::new();
        for task in &dag.tasks {
            if !task_ids.insert(&task.id) {
                return Err(anyhow!("Duplicate task ID: {}", task.id));
            }
        }

        // Check all dependencies reference existing tasks
        for task in &dag.tasks {
            for dep in &task.dependencies() {
                if !dag.tasks.iter().any(|t| &t.id == dep) {
                    return Err(anyhow!(
                        "Task {} depends on non-existent task: {}",
                        task.id,
                        dep
                    ));
                }
            }
        }

        // Validate cron schedule if present
        if let Some(schedule) = &dag.schedule {
            Self::validate_cron(schedule)?;
        }

        Ok(())
    }

    fn validate_cron(schedule: &str) -> Result<()> {
        use std::str::FromStr;
        
        let parts: Vec<&str> = schedule.split_whitespace().collect();
        let expression_to_test = match parts.len() {
            5 => format!("0 {} *", schedule),
            6 => format!("{} *", schedule),
            7 => schedule.to_string(),
            _ => return Err(anyhow!("Invalid cron expression length. Expected 5, 6, or 7 fields")),
        };

        if cron::Schedule::from_str(&expression_to_test).is_err() {
            return Err(anyhow!("Invalid cron expression: '{}'", schedule));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::definition::TaskDefinition;

    fn create_test_task(id: &str, depends_on: Option<Vec<String>>) -> TaskDefinition {
        TaskDefinition {
            id: id.to_string(),
            operator: "bash".to_string(),
            depends_on,
            retries: None,
            retry_delay_secs: None,
            timeout_secs: None,
            xcom_inputs: None,
            config: serde_json::json!({}),
        }
    }

    #[test]
    fn test_validate_valid_dag() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: Some("0 2 * * *".to_string()),
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                create_test_task("a", None),
                create_test_task("b", Some(vec!["a".to_string()])),
            ],
        };

        assert!(DagValidator::validate(&dag).is_ok());
    }

    #[test]
    fn test_validate_empty_id() {
        let dag = DagDefinition {
            id: "".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![create_test_task("a", None)],
        };

        assert!(DagValidator::validate(&dag).is_err());
    }

    #[test]
    fn test_validate_duplicate_task_ids() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                create_test_task("a", None),
                create_test_task("a", None),
            ],
        };

        assert!(DagValidator::validate(&dag).is_err());
    }

    #[test]
    fn test_validate_missing_dependency() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![
                create_test_task("a", None),
                create_test_task("b", Some(vec!["nonexistent".to_string()])),
            ],
        };

        assert!(DagValidator::validate(&dag).is_err());
    }

    #[test]
    fn test_validate_invalid_cron() {
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: Some("invalid cron".to_string()),
            max_active_runs: None,
            catchup: None,
            tasks: vec![create_test_task("a", None)],
        };

        assert!(DagValidator::validate(&dag).is_err());
    }
}
