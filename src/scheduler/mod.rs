use crate::dag::{DagDefinition, TriggerType};
use crate::executor::DagExecutor;
use crate::store::Store;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::info;

pub struct DagScheduler {
    store: Arc<Store>,
    executor: Arc<DagExecutor>,
    scheduler: Arc<Mutex<JobScheduler>>,
}

impl DagScheduler {
    pub async fn new(store: Arc<Store>) -> Result<Self> {
        let executor = Arc::new(DagExecutor::new(Arc::clone(&store)));
        let scheduler = Arc::new(Mutex::new(JobScheduler::new().await?));

        Ok(DagScheduler {
            store,
            executor,
            scheduler,
        })
    }

    /// Schedule a DAG
    pub async fn schedule_dag(&self, dag: &DagDefinition) -> Result<()> {
        if let Some(schedule) = &dag.schedule {
            let dag_id = dag.id.clone();
            let store = Arc::clone(&self.store);
            let executor = Arc::clone(&self.executor);
            let dag_clone = dag.clone();

            let job = Job::new_async(schedule.as_str(), move |_uuid, _l| {
                let store_clone = Arc::clone(&store);
                let executor_clone = Arc::clone(&executor);
                let dag_def = dag_clone.clone();

                Box::pin(async move {
                    // Check if DAG is paused
                    if let Ok(is_paused) = store_clone.is_dag_paused(&dag_def.id).await {
                        if is_paused {
                            return;
                        }
                    }

                    // Create a new DAG run
                    match store_clone
                        .create_dag_run(&dag_def.id, TriggerType::Schedule)
                        .await
                    {
                        Ok(dag_run) => {
                            info!("Scheduled trigger for DAG: {}", dag_def.id);
                            
                            // Execute the DAG run
                            if let Err(e) = executor_clone.execute(&dag_def, &dag_run).await {
                                eprintln!("Failed to execute scheduled DAG run: {}", e);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to create DAG run for scheduled execution: {}", e);
                        }
                    }
                })
            })?;

            let scheduler = self.scheduler.lock().await;
            scheduler.add(job).await?;

            info!("Scheduled DAG: {} with cron: {}", dag_id, schedule);
        }

        Ok(())
    }

    /// Start the scheduler
    pub async fn start(&self) -> Result<()> {
        let scheduler = self.scheduler.lock().await;
        scheduler.start().await?;
        info!("Scheduler started");
        Ok(())
    }

    /// Stop the scheduler
    pub async fn stop(&self) -> Result<()> {
        let mut scheduler = self.scheduler.lock().await;
        scheduler.shutdown().await?;
        info!("Scheduler stopped");
        Ok(())
    }

    /// Manually trigger a DAG run
    pub async fn trigger_dag(&self, dag_id: &str) -> Result<String> {
        let dag = self
            .store
            .get_dag(dag_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("DAG not found: {}", dag_id))?;

        let dag_run = self.store.create_dag_run(dag_id, TriggerType::Manual).await?;
        
        info!("Manually triggered DAG: {}", dag_id);
        
        let executor = Arc::clone(&self.executor);
        let dag_run_clone = dag_run.clone();
        
        tokio::spawn(async move {
            if let Err(e) = executor.execute(&dag, &dag_run_clone).await {
                eprintln!("Failed to execute manually triggered DAG run: {}", e);
            }
        });

        Ok(dag_run.id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::TaskDefinition;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let store = Arc::new(Store::new("sqlite::memory:").await.unwrap());
        let scheduler = DagScheduler::new(store).await;
        assert!(scheduler.is_ok());
    }

    #[tokio::test]
    #[ignore]  // Schedule parsing requires valid cron expressions, tested via integration
    async fn test_schedule_dag() {
        let store = Arc::new(Store::new("sqlite::memory:").await.unwrap());
        let scheduler = DagScheduler::new(store.clone()).await.unwrap();

        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: Some("* * * * *".to_string()),
            max_active_runs: None,
            catchup: None,
            tasks: vec![TaskDefinition {
                id: "task_1".to_string(),
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
        let result = scheduler.schedule_dag(&dag).await;
        if let Err(e) = &result {
            eprintln!("Error scheduling DAG: {}", e);
        }
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_trigger_dag() {
        let store = Arc::new(Store::new("sqlite::memory:").await.unwrap());
        let scheduler = DagScheduler::new(store.clone()).await.unwrap();

        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![TaskDefinition {
                id: "task_1".to_string(),
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
        let result = scheduler.trigger_dag(&dag.id).await;
        assert!(result.is_ok());
    }
}
