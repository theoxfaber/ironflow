use super::Operator;
use async_trait::async_trait;
use serde_json::Value;
use anyhow::Result;

pub struct SlackOperator;

#[async_trait]
impl Operator for SlackOperator {
    async fn execute(&self, config: &Value) -> Result<String> {
        let webhook_url = config
            .get("webhook_url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing 'webhook_url' field in Slack operator config"))?;

        let message = config
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("Task completed");

        let client = reqwest::Client::new();
        let payload = serde_json::json!({
            "text": message
        });

        let response = client.post(webhook_url).json(&payload).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Slack webhook request failed with status: {}",
                response.status()
            ));
        }

        Ok("Slack message sent".to_string())
    }

    fn name(&self) -> &'static str {
        "slack"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_slack_operator_missing_webhook() {
        let config = serde_json::json!({
            "message": "test"
        });

        let operator = SlackOperator;
        let result = operator.execute(&config).await;
        assert!(result.is_err());
    }
}
