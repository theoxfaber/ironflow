use super::Operator;
use async_trait::async_trait;
use serde_json::Value;
use anyhow::Result;
use std::process::Stdio;
use tokio::process::Command;

pub struct PythonOperator;

#[async_trait]
impl Operator for PythonOperator {
    async fn execute(&self, config: &Value) -> Result<String> {
        let script = config
            .get("script")
            .and_then(|v| v.as_str())
            .or_else(|| config.get("command").and_then(|v| v.as_str()))
            .ok_or_else(|| anyhow::anyhow!("Missing 'script' or 'command' field in python operator config"))?;

        let output = Command::new("python3")
            .arg("-c")
            .arg(script)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await?;

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "Python command failed with exit code: {:?}\nStderr: {}",
                output.status.code(),
                stderr
            ));
        }

        Ok(stdout)
    }

    fn name(&self) -> &'static str {
        "python"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_python_operator_success() {
        let config = serde_json::json!({
            "script": "print('hello world')"
        });

        let operator = PythonOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_ok());
        assert!(result.unwrap().contains("hello world"));
    }

    #[tokio::test]
    async fn test_python_operator_failure() {
        let config = serde_json::json!({
            "script": "raise Exception('test error')"
        });

        let operator = PythonOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_err());
    }
}
