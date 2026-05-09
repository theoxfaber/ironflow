use anyhow::Result;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use crate::dag::{DagDefinition, DagRun, DagRunStatus, TaskRun, TaskRunStatus, TriggerType};
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Create a new store and initialize the database
    pub async fn new(database_url: &str) -> Result<Self> {
        // Add mode=rwc if not already present to allow creating new databases
        let db_url = if database_url.contains("mode=") {
            database_url.to_string()
        } else {
            format!("{}?mode=rwc", database_url)
        };
        
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await?;

        // Enable Write-Ahead Logging for better concurrency
        sqlx::query("PRAGMA journal_mode = WAL;")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL;")
            .execute(&pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dags (
                id TEXT PRIMARY KEY,
                definition TEXT NOT NULL,
                is_paused BOOLEAN NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS dag_runs (
                id TEXT PRIMARY KEY,
                dag_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at DATETIME NOT NULL,
                ended_at DATETIME,
                triggered_by TEXT NOT NULL,
                run_number INTEGER NOT NULL,
                FOREIGN KEY (dag_id) REFERENCES dags(id)
            )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS task_runs (
                id TEXT PRIMARY KEY,
                dag_run_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at DATETIME,
                ended_at DATETIME,
                attempt_number INTEGER NOT NULL,
                log TEXT NOT NULL DEFAULT '',
                xcom_output TEXT,
                FOREIGN KEY (dag_run_id) REFERENCES dag_runs(id)
            )",
        )
        .execute(&pool)
        .await?;

        Ok(Store { pool })
    }

    // ===== DAG Operations =====

    pub async fn save_dag(&self, dag: &DagDefinition) -> Result<()> {
        let definition = serde_json::to_string(dag)?;
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        sqlx::query(
            "INSERT OR REPLACE INTO dags (id, definition, is_paused, created_at, updated_at)
             VALUES (?, ?, 0, ?, ?)",
        )
        .bind(&dag.id)
        .bind(&definition)
        .bind(&now_str)
        .bind(&now_str)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_dag(&self, dag_id: &str) -> Result<Option<DagDefinition>> {
        let row = sqlx::query("SELECT definition FROM dags WHERE id = ?")
            .bind(dag_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.and_then(|r| {
            let definition_str: String = r.get("definition");
            serde_json::from_str(&definition_str).ok()
        }))
    }

    pub async fn get_all_dags(&self) -> Result<Vec<DagDefinition>> {
        let rows = sqlx::query("SELECT definition FROM dags")
            .fetch_all(&self.pool)
            .await?;

        let dags = rows
            .into_iter()
            .filter_map(|row| {
                let definition_str: String = row.get("definition");
                serde_json::from_str(&definition_str).ok()
            })
            .collect();

        Ok(dags)
    }

    pub async fn pause_dag(&self, dag_id: &str) -> Result<()> {
        sqlx::query("UPDATE dags SET is_paused = 1 WHERE id = ?")
            .bind(dag_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn unpause_dag(&self, dag_id: &str) -> Result<()> {
        sqlx::query("UPDATE dags SET is_paused = 0 WHERE id = ?")
            .bind(dag_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn is_dag_paused(&self, dag_id: &str) -> Result<bool> {
        let row = sqlx::query("SELECT is_paused FROM dags WHERE id = ?")
            .bind(dag_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|r| r.get::<bool, _>("is_paused")).unwrap_or(false))
    }

    /// Recover orphaned DAG and task runs from a previous crash
    /// Marks any Running tasks/runs as Failed with a system message
    pub async fn recover_orphaned_runs(&self) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        
        // Find and mark orphaned task runs as Failed
        let orphaned_tasks = sqlx::query(
            "SELECT id FROM task_runs WHERE status = ?",
        )
        .bind(TaskRunStatus::Running.to_string())
        .fetch_all(&self.pool)
        .await?;

        for task_row in orphaned_tasks {
            let task_run_id: String = task_row.get("id");
            let recovery_msg = "Orphaned by executor crash — marked failed on restart";
            
            sqlx::query(
                "UPDATE task_runs SET status = ?, ended_at = ?, log = log || '\n' || ? WHERE id = ?",
            )
            .bind(TaskRunStatus::Failed.to_string())
            .bind(&now)
            .bind(recovery_msg)
            .bind(&task_run_id)
            .execute(&self.pool)
            .await?;
            
            tracing::info!("Recovered orphaned task run: {}", task_run_id);
        }

        // Find and mark orphaned DAG runs as Failed
        let orphaned_runs = sqlx::query(
            "SELECT id FROM dag_runs WHERE status = ?",
        )
        .bind(DagRunStatus::Running.to_string())
        .fetch_all(&self.pool)
        .await?;

        for run_row in orphaned_runs {
            let dag_run_id: String = run_row.get("id");
            
            sqlx::query(
                "UPDATE dag_runs SET status = ?, ended_at = ? WHERE id = ?",
            )
            .bind(DagRunStatus::Failed.to_string())
            .bind(&now)
            .bind(&dag_run_id)
            .execute(&self.pool)
            .await?;
            
            tracing::info!("Recovered orphaned DAG run: {}", dag_run_id);
        }

        Ok(())
    }

    // ===== DAG Run Operations =====

    pub async fn create_dag_run(
        &self,
        dag_id: &str,
        triggered_by: TriggerType,
    ) -> Result<DagRun> {
        let run_id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let now_str = now.to_rfc3339();

        // Get the next run number for this DAG
        let run_number: i64 = sqlx::query_scalar(
            "SELECT COALESCE(MAX(run_number), 0) + 1 FROM dag_runs WHERE dag_id = ?",
        )
        .bind(dag_id)
        .fetch_one(&self.pool)
        .await?;

        sqlx::query(
            "INSERT INTO dag_runs (id, dag_id, status, started_at, triggered_by, run_number)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(&run_id)
        .bind(dag_id)
        .bind(DagRunStatus::Queued.to_string())
        .bind(&now_str)
        .bind(triggered_by.to_string())
        .bind(run_number)
        .execute(&self.pool)
        .await?;

        Ok(DagRun {
            id: run_id,
            dag_id: dag_id.to_string(),
            status: DagRunStatus::Queued,
            started_at: now,
            ended_at: None,
            triggered_by,
            run_number: run_number as u32,
        })
    }

    pub async fn get_dag_run(&self, run_id: &str) -> Result<Option<DagRun>> {
        let row = sqlx::query(
            "SELECT id, dag_id, status, started_at, ended_at, triggered_by, run_number
             FROM dag_runs WHERE id = ?",
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.and_then(|r| {
            let status_str: String = r.get("status");
            let triggered_str: String = r.get("triggered_by");
            let started_at_str: String = r.get("started_at");
            let started_at = DateTime::parse_from_rfc3339(&started_at_str)
                .ok()?
                .with_timezone(&Utc);

            Some(DagRun {
                id: r.get("id"),
                dag_id: r.get("dag_id"),
                status: match status_str.as_str() {
                    "queued" => DagRunStatus::Queued,
                    "running" => DagRunStatus::Running,
                    "success" => DagRunStatus::Success,
                    "failed" => DagRunStatus::Failed,
                    _ => DagRunStatus::Queued,
                },
                started_at,
                ended_at: {
                    let ended_at_str: Option<String> = r.get("ended_at");
                    ended_at_str.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    })
                },
                triggered_by: match triggered_str.as_str() {
                    "schedule" => TriggerType::Schedule,
                    "manual" => TriggerType::Manual,
                    _ => TriggerType::Manual,
                },
                run_number: r.get::<i64, _>("run_number") as u32,
            })
        }))
    }

    pub async fn get_dag_runs(&self, dag_id: &str, limit: i64) -> Result<Vec<DagRun>> {
        let rows = sqlx::query(
            "SELECT id, dag_id, status, started_at, ended_at, triggered_by, run_number
             FROM dag_runs WHERE dag_id = ? ORDER BY started_at DESC LIMIT ?",
        )
        .bind(dag_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let runs = rows
            .into_iter()
            .filter_map(|r| {
                let status_str: String = r.get("status");
                let triggered_str: String = r.get("triggered_by");
                let started_at_str: String = r.get("started_at");
                let started_at = DateTime::parse_from_rfc3339(&started_at_str)
                    .ok()?
                    .with_timezone(&Utc);

                Some(DagRun {
                    id: r.get("id"),
                    dag_id: r.get("dag_id"),
                    status: match status_str.as_str() {
                        "queued" => DagRunStatus::Queued,
                        "running" => DagRunStatus::Running,
                        "success" => DagRunStatus::Success,
                        "failed" => DagRunStatus::Failed,
                        _ => DagRunStatus::Queued,
                    },
                    started_at,
                    ended_at: {
                        let ended_at_str: Option<String> = r.get("ended_at");
                        ended_at_str.and_then(|s| {
                            DateTime::parse_from_rfc3339(&s)
                                .ok()
                                .map(|dt| dt.with_timezone(&Utc))
                        })
                    },
                    triggered_by: match triggered_str.as_str() {
                        "schedule" => TriggerType::Schedule,
                        "manual" => TriggerType::Manual,
                        _ => TriggerType::Manual,
                    },
                    run_number: r.get::<i64, _>("run_number") as u32,
                })
            })
            .collect();

        Ok(runs)
    }

    pub async fn update_dag_run_status(&self, run_id: &str, status: DagRunStatus) -> Result<()> {
        let ended_at = if matches!(status, DagRunStatus::Success | DagRunStatus::Failed) {
            Some(Utc::now().to_rfc3339())
        } else {
            None
        };

        sqlx::query(
            "UPDATE dag_runs SET status = ?, ended_at = ? WHERE id = ?",
        )
        .bind(status.to_string())
        .bind(ended_at)
        .bind(run_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    // ===== Task Run Operations =====

    pub async fn create_task_run(
        &self,
        dag_run_id: &str,
        task_id: &str,
    ) -> Result<TaskRun> {
        let task_run_id = Uuid::new_v4().to_string();

        sqlx::query(
            "INSERT INTO task_runs (id, dag_run_id, task_id, status, attempt_number, log)
             VALUES (?, ?, ?, ?, 1, '')",
        )
        .bind(&task_run_id)
        .bind(dag_run_id)
        .bind(task_id)
        .bind(TaskRunStatus::Pending.to_string())
        .execute(&self.pool)
        .await?;

        Ok(TaskRun {
            id: task_run_id,
            dag_run_id: dag_run_id.to_string(),
            task_id: task_id.to_string(),
            status: TaskRunStatus::Pending,
            started_at: None,
            ended_at: None,
            attempt_number: 1,
            log: String::new(),
            xcom_output: None,
        })
    }

    pub async fn get_task_run(&self, task_run_id: &str) -> Result<Option<TaskRun>> {
        let row = sqlx::query(
            "SELECT id, dag_run_id, task_id, status, started_at, ended_at, attempt_number, log, xcom_output
             FROM task_runs WHERE id = ?",
        )
        .bind(task_run_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            let status_str: String = r.get("status");
            let started_at_str: Option<String> = r.get("started_at");
            let ended_at_str: Option<String> = r.get("ended_at");

            TaskRun {
                id: r.get("id"),
                dag_run_id: r.get("dag_run_id"),
                task_id: r.get("task_id"),
                status: match status_str.as_str() {
                    "pending" => TaskRunStatus::Pending,
                    "running" => TaskRunStatus::Running,
                    "success" => TaskRunStatus::Success,
                    "failed" => TaskRunStatus::Failed,
                    "retried" => TaskRunStatus::Retried,
                    "skipped" => TaskRunStatus::Skipped,
                    _ => TaskRunStatus::Pending,
                },
                started_at: started_at_str.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc))
                }),
                ended_at: ended_at_str.and_then(|s| {
                    DateTime::parse_from_rfc3339(&s)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc))
                }),
                attempt_number: r.get::<i64, _>("attempt_number") as u32,
                log: r.get("log"),
                xcom_output: r.get("xcom_output"),
            }
        }))
    }

    pub async fn get_task_runs_for_dag_run(&self, dag_run_id: &str) -> Result<Vec<TaskRun>> {
        let rows = sqlx::query(
            "SELECT id, dag_run_id, task_id, status, started_at, ended_at, attempt_number, log, xcom_output
             FROM task_runs WHERE dag_run_id = ?",
        )
        .bind(dag_run_id)
        .fetch_all(&self.pool)
        .await?;

        let task_runs = rows
            .into_iter()
            .map(|r| {
                let status_str: String = r.get("status");
                let started_at_str: Option<String> = r.get("started_at");
                let ended_at_str: Option<String> = r.get("ended_at");

                TaskRun {
                    id: r.get("id"),
                    dag_run_id: r.get("dag_run_id"),
                    task_id: r.get("task_id"),
                    status: match status_str.as_str() {
                        "pending" => TaskRunStatus::Pending,
                        "running" => TaskRunStatus::Running,
                        "success" => TaskRunStatus::Success,
                        "failed" => TaskRunStatus::Failed,
                        "retried" => TaskRunStatus::Retried,
                        "skipped" => TaskRunStatus::Skipped,
                        _ => TaskRunStatus::Pending,
                    },
                    started_at: started_at_str.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    }),
                    ended_at: ended_at_str.and_then(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc))
                    }),
                    attempt_number: r.get::<i64, _>("attempt_number") as u32,
                    log: r.get("log"),
                    xcom_output: r.get("xcom_output"),
                }
            })
            .collect();

        Ok(task_runs)
    }

    pub async fn update_task_run(
        &self,
        task_run_id: &str,
        status: TaskRunStatus,
        log_append: Option<&str>,
        xcom_output: Option<String>,
    ) -> Result<()> {
        let started_at = if matches!(status, TaskRunStatus::Running) {
            Some(Utc::now().to_rfc3339())
        } else {
            None
        };

        let ended_at = if matches!(
            status,
            TaskRunStatus::Success | TaskRunStatus::Failed | TaskRunStatus::Skipped
        ) {
            Some(Utc::now().to_rfc3339())
        } else {
            None
        };

        // Get current log and append
        let mut new_log = String::new();
        if let Some(append) = log_append {
            if let Ok(Some(task_run)) = self.get_task_run(task_run_id).await {
                new_log = format!("{}\n{}", task_run.log, append);
            } else {
                new_log = append.to_string();
            }
        }

        sqlx::query(
            "UPDATE task_runs SET status = ?, started_at = COALESCE(started_at, ?), 
             ended_at = ?, log = CASE WHEN ? THEN ? ELSE log END, xcom_output = COALESCE(?, xcom_output)
             WHERE id = ?",
        )
        .bind(status.to_string())
        .bind(&started_at)
        .bind(&ended_at)
        .bind(!new_log.is_empty())
        .bind(&new_log)
        .bind(&xcom_output)
        .bind(task_run_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn increment_task_run_attempt(&self, task_run_id: &str) -> Result<u32> {
        let new_attempt: i64 = sqlx::query_scalar(
            "UPDATE task_runs SET attempt_number = attempt_number + 1 WHERE id = ? RETURNING attempt_number",
        )
        .bind(task_run_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(new_attempt as u32)
    }

    pub async fn append_task_log(&self, task_run_id: &str, log_line: &str) -> Result<()> {
        if let Ok(Some(task_run)) = self.get_task_run(task_run_id).await {
            let new_log = format!("{}\n{}", task_run.log, log_line);
            sqlx::query("UPDATE task_runs SET log = ? WHERE id = ?")
                .bind(&new_log)
                .bind(task_run_id)
                .execute(&self.pool)
                .await?;
        }

        Ok(())
    }

    // ===== XCom Operations =====

    /// Get XCom output for a task in a specific run
    pub async fn get_xcom(&self, run_id: &str, task_id: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT xcom_output FROM task_runs WHERE dag_run_id = ? AND task_id = ? AND status = ?",
        )
        .bind(run_id)
        .bind(task_id)
        .bind(TaskRunStatus::Success.to_string())
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get::<Option<String>, _>("xcom_output")).flatten())
    }

    /// Get all XCom outputs for a DAG run, organized by task_id
    pub async fn get_all_xcoms_for_run(&self, run_id: &str) -> Result<serde_json::Map<String, serde_json::Value>> {
        let rows = sqlx::query(
            "SELECT task_id, xcom_output FROM task_runs WHERE dag_run_id = ? AND status = ? AND xcom_output IS NOT NULL",
        )
        .bind(run_id)
        .bind(TaskRunStatus::Success.to_string())
        .fetch_all(&self.pool)
        .await?;

        let mut xcoms = serde_json::Map::new();
        for row in rows {
            let task_id: String = row.get("task_id");
            let xcom_output: Option<String> = row.get("xcom_output");
            if let Some(output) = xcom_output {
                xcoms.insert(task_id, serde_json::json!(output));
            }
        }

        Ok(xcoms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_creation() {
        let store = Store::new("sqlite::memory:").await;
        assert!(store.is_ok());
    }

    #[tokio::test]
    async fn test_save_and_get_dag() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: Some("Test".to_string()),
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        let retrieved = store.get_dag("test_dag").await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "test_dag");
    }

    #[tokio::test]
    async fn test_dag_run_creation_and_retrieval() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        
        let dag_run = store
            .create_dag_run("test_dag", TriggerType::Manual)
            .await
            .unwrap();

        assert_eq!(dag_run.dag_id, "test_dag");
        assert_eq!(dag_run.status, DagRunStatus::Queued);

        let retrieved = store.get_dag_run(&dag_run.id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_task_run_creation_and_update() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        
        let dag_run = store
            .create_dag_run("test_dag", TriggerType::Manual)
            .await
            .unwrap();

        let task_run = store
            .create_task_run(&dag_run.id, "task_1")
            .await
            .unwrap();

        assert_eq!(task_run.status, TaskRunStatus::Pending);

        store
            .update_task_run(&task_run.id, TaskRunStatus::Running, None, None)
            .await
            .unwrap();

        let retrieved = store.get_task_run(&task_run.id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, TaskRunStatus::Running);
    }

    #[tokio::test]
    async fn test_crash_recovery_orphaned_runs() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        
        // Create a DAG run and mark it as Running (simulating a crash)
        let dag_run = store
            .create_dag_run("test_dag", TriggerType::Manual)
            .await
            .unwrap();
        
        store
            .update_dag_run_status(&dag_run.id, DagRunStatus::Running)
            .await
            .unwrap();

        let task_run = store
            .create_task_run(&dag_run.id, "task_1")
            .await
            .unwrap();

        store
            .update_task_run(&task_run.id, TaskRunStatus::Running, None, None)
            .await
            .unwrap();

        // Verify they are Running
        let dag_run_before = store.get_dag_run(&dag_run.id).await.unwrap().unwrap();
        assert_eq!(dag_run_before.status, DagRunStatus::Running);
        
        let task_run_before = store.get_task_run(&task_run.id).await.unwrap().unwrap();
        assert_eq!(task_run_before.status, TaskRunStatus::Running);

        // Now recover from crash
        store.recover_orphaned_runs().await.unwrap();

        // Verify they are now Failed
        let dag_run_after = store.get_dag_run(&dag_run.id).await.unwrap().unwrap();
        assert_eq!(dag_run_after.status, DagRunStatus::Failed);
        assert!(dag_run_after.ended_at.is_some());
        
        let task_run_after = store.get_task_run(&task_run.id).await.unwrap().unwrap();
        assert_eq!(task_run_after.status, TaskRunStatus::Failed);
        assert!(task_run_after.ended_at.is_some());
        assert!(task_run_after.log.contains("Orphaned by executor crash"));
    }

    #[tokio::test]
    async fn test_xcom_get_and_retrieve() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        let dag_run = store
            .create_dag_run("test_dag", TriggerType::Manual)
            .await
            .unwrap();

        let task_run = store
            .create_task_run(&dag_run.id, "task_1")
            .await
            .unwrap();

        // Simulate task execution with XCom output
        let xcom_output = r#"{"result": "success", "count": 42}"#.to_string();
        store
            .update_task_run(&task_run.id, TaskRunStatus::Success, Some("Task completed"), Some(xcom_output.clone()))
            .await
            .unwrap();

        // Retrieve XCom output
        let retrieved_xcom = store.get_xcom(&dag_run.id, "task_1").await.unwrap();
        assert!(retrieved_xcom.is_some());
        assert_eq!(retrieved_xcom.unwrap(), xcom_output);
    }

    #[tokio::test]
    async fn test_get_all_xcoms_for_run() {
        let store = Store::new("sqlite::memory:").await.unwrap();
        let dag = DagDefinition {
            id: "test_dag".to_string(),
            description: None,
            schedule: None,
            max_active_runs: None,
            catchup: None,
            tasks: vec![],
        };

        store.save_dag(&dag).await.unwrap();
        let dag_run = store
            .create_dag_run("test_dag", TriggerType::Manual)
            .await
            .unwrap();

        // Create multiple tasks with XCom outputs
        let task1 = store.create_task_run(&dag_run.id, "task_1").await.unwrap();
        let task2 = store.create_task_run(&dag_run.id, "task_2").await.unwrap();

        store
            .update_task_run(&task1.id, TaskRunStatus::Success, None, Some(r#"{"value": 1}"#.to_string()))
            .await
            .unwrap();

        store
            .update_task_run(&task2.id, TaskRunStatus::Success, None, Some(r#"{"value": 2}"#.to_string()))
            .await
            .unwrap();

        // Retrieve all XComs
        let all_xcoms = store.get_all_xcoms_for_run(&dag_run.id).await.unwrap();
        assert_eq!(all_xcoms.len(), 2);
        assert!(all_xcoms.contains_key("task_1"));
        assert!(all_xcoms.contains_key("task_2"));
    }
}