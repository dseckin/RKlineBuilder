use serde::Serialize;
use redis::AsyncCommands;
use rmp_serde::to_vec_named;
use anyhow::Result;

#[derive(Serialize)]
pub struct Kline {
    pub open_time: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub asset_volume: f64,
}

#[derive(Serialize)]
pub struct IndicatorOutput {
    pub klines: Vec<Kline>,
    pub kama_21: Vec<f64>,
    pub kama_34: Vec<f64>,
    pub kama_roc_1: Vec<f64>,
    pub cci_34: Vec<f64>,
    pub cci_170: Vec<f64>,
    pub donchian_upper: Vec<f64>,
    pub donchian_lower: Vec<f64>,
    pub ichimoku_span_a: Vec<f64>,
    pub ichimoku_span_b: Vec<f64>,
    pub donc_position: Vec<f64>,
    pub kama_donc_position: Vec<f64>,
    pub close_kama_position: Vec<f64>,
}

pub async fn write_indicators_to_redis(
    redis_client: &redis::Client,
    symbol: &str,
    interval: &str,
    output: &IndicatorOutput,
) -> Result<()> {
    let mut conn = redis_client.get_tokio_connection().await?;
    let key = format!("{}klines{}", symbol, interval);
    let packed = to_vec_named(output)?;
    conn.set(key, packed).await?;
    Ok(())
}
