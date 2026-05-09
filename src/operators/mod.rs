use async_trait::async_trait;
use serde_json::Value;
use anyhow::Result;

pub mod bash;
pub mod python;
pub mod http;
pub mod sql;
pub mod slack;

pub use bash::BashOperator;
pub use python::PythonOperator;
pub use http::HttpOperator;
pub use sql::SqlOperator;
pub use slack::SlackOperator;

#[async_trait]
pub trait Operator: Send + Sync {
    /// Execute the operator task
    async fn execute(&self, config: &Value) -> Result<String>;
    
    /// Get the operator name
    fn name(&self) -> &'static str;
}

pub struct OperatorRegistry;

impl OperatorRegistry {
    pub fn get_operator(operator_type: &str) -> Option<Box<dyn Operator>> {
        match operator_type {
            "bash" => Some(Box::new(BashOperator)),
            "python" => Some(Box::new(PythonOperator)),
            "http" => Some(Box::new(HttpOperator)),
            "sql" => Some(Box::new(SqlOperator)),
            "slack" => Some(Box::new(SlackOperator)),
            _ => None,
        }
    }
}
