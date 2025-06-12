use redis::{AsyncCommands, aio::ConnectionLike, RedisResult, cmd};
use serde::{Deserialize};
use anyhow::Result;
use chrono::{Utc, Timelike, Datelike, Duration};

use crate::indicators::Candle;

#[derive(Debug, Clone, Deserialize)]
struct RawCandle {
    open_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    asset_volume: f64,
}

pub async fn fetch_candles_batch_from_redis(
    redis_client: &redis::Client,
    symbol: &str,
    interval: &str,
    kline_count: usize,
    gmt_offset: i32 // örn: +2
) -> Result<Vec<Candle>> {
    let mut conn = redis_client.get_tokio_connection().await?;
    let now = Utc::now() + Duration::hours(gmt_offset as i64);
    
    let mut keys = Vec::new();
    for i in 0..kline_count {
        let time = now - Duration::minutes(i as i64);
        let key = format!(
            "{}_{}_{}_{}_{}_{}_{}",
            symbol,
            time.year(),
            pad(time.month()),
            pad(time.day()),
            pad(time.hour()),
            pad(time.minute()),
            interval
        );
        keys.push(key);
    }

    // MGET komutu
    let mut redis_cmd = cmd("MGET");
    for key in &keys {
        redis_cmd.arg(key);
    }
    let results: RedisResult<Vec<Option<Vec<u8>>>> = redis_cmd.query_async(&mut conn).await;

    let mut candles = Vec::new();
    for value in results?.into_iter().rev() {
        if let Some(bytes) = value {
            if let Ok(raw_candle) = rmp_serde::from_slice::<RawCandle>(&bytes) {
                candles.push(Candle {
                    open_time: raw_candle.open_time,
                    open: raw_candle.open,
                    high: raw_candle.high,
                    low: raw_candle.low,
                    close: raw_candle.close,
                    volume: raw_candle.volume,
                    asset_volume: raw_candle.asset_volume,
                });
            }
        }
    }

    Ok(candles)
}

fn pad(n: u32) -> String {
    format!("{:02}", n)
}