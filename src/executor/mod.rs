use crate::dag::{DagDefinition, DagRun, TaskRunStatus};
use crate::operators::OperatorRegistry;
use crate::store::Store;
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

pub struct DagExecutor {
    store: std::sync::Arc<Store>,
}

impl DagExecutor {
    pub fn new(store: std::sync::Arc<Store>) -> Self {
        DagExecutor { store }
    }

    /// Execute a DAG run
    pub async fn execute(&self, dag: &DagDefinition, dag_run: &DagRun) -> Result<()> {
        info!("Starting execution of DAG run: {}", dag_run.id);

        // Mark DAG run as running
        self.store
            .update_dag_run_status(&dag_run.id, crate::dag::DagRunStatus::Running)
            .await?;

        // Create task runs for all tasks
        let mut task_runs = HashMap::new();
        for task in &dag.tasks {
            let task_run = self.store.create_task_run(&dag_run.id, &task.id).await?;
            task_runs.insert(task.id.clone(), task_run);
        }

        // Execute tasks respecting dependencies
        let mut completed_tasks = HashSet::new();
        let mut failed_tasks = HashSet::new();
        let mut running_tasks = HashSet::new();
        let mut join_set = tokio::task::JoinSet::new();

        loop {
            // Find tasks that can run (all dependencies satisfied)
            let runnable_tasks: Vec<String> = dag
                .tasks
                .iter()
                .filter(|task| {
                    !completed_tasks.contains(&task.id)
                        && !failed_tasks.contains(&task.id)
                        && !running_tasks.contains(&task.id)
                        && dag.dependencies_satisfied(&task.id, &completed_tasks)
                })
                .map(|t| t.id.clone())
                .collect();

            // Spawn runnable tasks into the JoinSet
            for task_id in runnable_tasks {
                running_tasks.insert(task_id.clone());
                
                let task = dag.get_task(&task_id).unwrap();
                let task_run = task_runs[&task_id].clone();
                let store = std::sync::Arc::clone(&self.store);
                let dag_def = dag.clone();
                let task_def = task.clone();
                let task_id_clone = task_id.clone();
                let dag_run_id = dag_run.id.clone();

                join_set.spawn(async move {
                    (
                        task_id_clone,
                        Self::execute_task(&store, &dag_def, &dag_run_id, &task_run, &task_def).await,
                    )
                });
            }

            // If nothing is running and nothing is runnable, we are done
            if join_set.is_empty() {
                break;
            }

            // Wait for the next task to complete
            if let Some(res) = join_set.join_next().await {
                let (task_id, result) = res?;
                running_tasks.remove(&task_id);
                
                if result.is_ok() {
                    completed_tasks.insert(task_id);
                } else {
                    failed_tasks.insert(task_id);
                }
            }
        }

        // Determine overall DAG run status
        let dag_status = if failed_tasks.is_empty() {
            crate::dag::DagRunStatus::Success
        } else {
            crate::dag::DagRunStatus::Failed
        };

        self.store
            .update_dag_run_status(&dag_run.id, dag_status)
            .await?;

        info!("Completed execution of DAG run: {}", dag_run.id);
        Ok(())
    }

    async fn execute_task(
        store: &std::sync::Arc<Store>,
        _dag: &DagDefinition,
        dag_run_id: &str,
        task_run: &crate::dag::TaskRun,
        task_def: &crate::dag::TaskDefinition,
    ) -> Result<()> {
        let mut attempt = task_run.attempt_number;
        let max_attempts = task_def.retries.unwrap_or(0) + 1;

        loop {
            info!("Executing task: {} (attempt {}/{})", task_def.id, attempt, max_attempts);

            // Mark task as running
            store
                .update_task_run(&task_run.id, TaskRunStatus::Running, None, None)
                .await?;

            // Prepare task config with XCom injections
            let mut task_config = task_def.config.clone();
            
            // Inject XCom outputs from upstream tasks
            for upstream_task_id in task_def.xcom_dependencies() {
                if let Ok(Some(xcom_output)) = store.get_xcom(dag_run_id, &upstream_task_id).await {
                    // Parse the XCom output as JSON
                    if let Ok(xcom_json) = serde_json::from_str::<serde_json::Value>(&xcom_output) {
                        // Inject under xcom.<task_id> key
                        if !task_config.is_object() {
                            task_config = serde_json::json!({});
                        }
                        if let Some(obj) = task_config.as_object_mut() {
                            if !obj.contains_key("xcom") {
                                obj.insert("xcom".to_string(), serde_json::json!({}));
                            }
                            if let Some(xcom_obj) = obj.get_mut("xcom").and_then(|x| x.as_object_mut()) {
                                xcom_obj.insert(upstream_task_id.clone(), xcom_json);
                            }
                        }
                    }
                }
            }

            // Get the operator
            let operator = OperatorRegistry::get_operator(&task_def.operator)
                .ok_or_else(|| anyhow::anyhow!("Unknown operator: {}", task_def.operator))?;

            // Execute the operator with timeout
            let timeout_secs = task_def.timeout_secs.unwrap_or(3600); // 1 hour default
            let execution_result = tokio::time::timeout(
                tokio::time::Duration::from_secs(timeout_secs),
                operator.execute(&task_config)
            ).await;

            let final_result = match execution_result {
                Ok(res) => res,
                Err(_) => Err(anyhow::anyhow!("Task execution timed out after {} seconds", timeout_secs)),
            };

            match final_result {
                Ok(output) => {
                    info!("Task {} succeeded", task_def.id);
                    let output_clone = output.clone();
                    store
                        .update_task_run(
                            &task_run.id,
                            TaskRunStatus::Success,
                            Some(&output),
                            Some(output_clone),
                        )
                        .await?;
                    return Ok(());
                }
                Err(e) => {
                    warn!("Task {} failed (attempt {}/{}): {}", task_def.id, attempt, max_attempts, e);

                    if attempt < max_attempts {
                        store
                            .update_task_run(
                                &task_run.id,
                                TaskRunStatus::Retried,
                                Some(&e.to_string()),
                                None,
                            )
                            .await?;

                        // Wait before retrying
                        let delay = task_def.retry_delay_secs.unwrap_or(60);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay)).await;

                        // Increment attempt and retry
                        attempt += 1;
                        store.increment_task_run_attempt(&task_run.id).await?;
                        continue;
                    } else {
                        store
                            .update_task_run(
                                &task_run.id,
                                TaskRunStatus::Failed,
                                Some(&e.to_string()),
                                None,
                            )
                            .await?;
                        return Err(e);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::{TaskDefinition, TriggerType};

    #[tokio::test]
    async fn test_executor_simple_dag() {
        let store = std::sync::Arc::new(Store::new("sqlite::memory:").await.unwrap());

        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![TaskDefinition {
                id: "simple_task".to_string(),
                operator: "bash".to_string(),
                depends_on: None,
                retries: None,
                retry_delay_secs: None,
                timeout_secs: None,
                xcom_inputs: None,
                config: serde_json::json!({
                    "command": "echo 'test'"
                }),
            }],
        };

        store.save_dag(&dag).await.unwrap();
        let dag_run = store.create_dag_run(&dag.id, TriggerType::Manual).await.unwrap();

        let executor = DagExecutor::new(std::sync::Arc::clone(&store));
        let result = executor.execute(&dag, &dag_run).await;

        assert!(result.is_ok());
    }
}
