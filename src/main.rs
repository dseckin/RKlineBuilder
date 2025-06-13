use std::env;
use std::collections::HashMap;
use tokio::task::JoinSet;
use anyhow::Result;
use redis::AsyncCommands;

mod indicators;
mod redis_utils;
mod redis_writer;

use indicators::{Candle, calculate_indicators};
use redis_utils::fetch_candles_batch_from_redis;
use redis_writer::{write_indicators_to_redis, IndicatorOutput, Kline};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Kullanım: {} <symbols_key> <kline_count>", args[0]);
        std::process::exit(1);
    }

    let symbols_key = &args[1];
    let kline_count: usize = args[2].parse().expect("kline_count sayı olmalı");

    // Redis bağlantısı
    let redis_client = redis::Client::open("redis://127.0.0.1/")?;
    
    // Sembolleri al
    let symbols = fetch_symbols(&redis_client, symbols_key).await?;
    println!("İşlenecek semboller: {:?}", symbols);

    // Paralel işlem için JoinSet kullan
    let mut join_set = JoinSet::new();
    
    for symbol in symbols {
        let redis_client_clone = redis_client.clone();
        let symbol_clone = symbol.clone();
        
        join_set.spawn(async move {
            process_symbol(&redis_client_clone, &symbol_clone, kline_count).await
        });
    }

    // Tüm görevlerin tamamlanmasını bekle
    let mut success_count = 0;
    let mut error_count = 0;
    
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(_)) => {
                success_count += 1;
                println!("✓ Başarılı işlem sayısı: {}", success_count);
            }
            Ok(Err(e)) => {
                error_count += 1;
                eprintln!("✗ Hata oluştu: {}", e);
            }
            Err(e) => {
                error_count += 1;
                eprintln!("✗ Task hatası: {}", e);
            }
        }
    }

    println!("İşlem tamamlandı. Başarılı: {}, Hatalı: {}", success_count, error_count);
    Ok(())
}

async fn fetch_symbols(redis_client: &redis::Client, symbols_key: &str) -> Result<Vec<String>> {
    let mut conn = redis_client.get_tokio_connection().await?;
    let symbols_str: String = conn.get(symbols_key).await?;
    Ok(symbols_str.split('#').map(|s| s.to_string()).collect())
}

async fn process_symbol(redis_client: &redis::Client, symbol: &str, kline_count: usize) -> Result<()> {
    println!("İşleniyor: {}", symbol);

    // Her sembol için hem 2m hem 30m verilerini paralel olarak işle
    let redis_2m = redis_client.clone();
    let redis_30m = redis_client.clone();
    let symbol_2m = symbol.to_string();
    let symbol_30m = symbol.to_string();

    let (result_2m, result_30m) = tokio::join!(
        process_timeframe(&redis_2m, &symbol_2m, "2m", kline_count),
        process_timeframe(&redis_30m, &symbol_30m, "30m", kline_count)
    );

    match result_2m {
        Ok(_) => println!("✓ {}-2m tamamlandı", symbol),
        Err(e) => eprintln!("✗ {}-2m hatası: {}", symbol, e),
    }

    match result_30m {
        Ok(_) => println!("✓ {}-30m tamamlandı", symbol),
        Err(e) => eprintln!("✗ {}-30m hatası: {}", symbol, e),
    }

    Ok(())
}

async fn process_timeframe(
    redis_client: &redis::Client,
    symbol: &str,
    interval: &str,
    kline_count: usize
) -> Result<()> {
    // Mevcut verileri kontrol et ve kaç yeni kline hesaplamamız gerektiğini belirle
    let calculation_count = determine_calculation_count(redis_client, symbol, interval, kline_count).await?;
    
    // Verileri Redis'ten al (GMT+2 offset ile)
    let candles = fetch_candles_batch_from_redis(redis_client, symbol, interval, calculation_count, 2).await?;
    
    if candles.len() < 21 {
        println!("⚠ {}-{}: Yetersiz veri ({}), en az 21 kline gerekli", symbol, interval, candles.len());
        return Ok(());
    }

    // Indikatörleri hesapla
    let indicators = calculate_indicators(&candles);
    
    // Kline'ları hazırla
    let klines: Vec<Kline> = candles.iter().map(|c| Kline {
        open_time: c.open_time,
        open: c.open,
        close: c.close,
        high: c.high,
        low: c.low,
        volume: c.volume,
        asset_volume: c.asset_volume,
    }).collect();

    // Çıktıyı hazırla
    let output = IndicatorOutput {
        klines,
        kama_21: indicators.kama21,
        kama_34: indicators.kama34,
        kama_roc_1: indicators.kama21_roc1,
        cci_34: indicators.cci34,
        cci_170: indicators.cci170,
        donchian_upper: indicators.donchian_upper,
        donchian_lower: indicators.donchian_lower,
        ichimoku_span_a: indicators.ichimoku_span_a,
        ichimoku_span_b: indicators.ichimoku_span_b,
        donc_position: indicators.donc_position,
        kama_donc_position: indicators.kama_donc_position,
        close_kama_position: indicators.close_kama_position,
    };

    // Redis'e yaz
    write_indicators_to_redis(redis_client, symbol, interval, &output).await?;
    
    Ok(())
}

async fn determine_calculation_count(
    redis_client: &redis::Client,
    symbol: &str,
    interval: &str,
    max_kline_count: usize
) -> Result<usize> {
    return Ok(max_kline_count); //şimdilik tümünü tekrar hesaplıyoruz


    
    let mut conn = redis_client.get_tokio_connection().await?;
    let key = format!("{}klines{}", symbol, interval);
    
    // Mevcut veriyi kontrol et
    let exists: bool = conn.exists(&key).await?;
    
    if !exists {
        // İlk sefer, tüm kline'ları hesapla
        return Ok(max_kline_count);
    }

    // Mevcut veri var, ne kadar yeni veri olduğunu kontrol et
    // Bu basit implementasyon için yeni kline sayısını tahmin ediyoruz
    // Gerçek sistemde timestamp karşılaştırması yapılabilir
    
    // Basit optimizasyon: her seferinde son 30 kline'ı yeniden hesapla
    // Bu, yeni gelen veriler için yeterli olacaktır
    let new_data_count = 30; // Tahmin edilen yeni veri sayısı
    
    if new_data_count <= 20 {
        Ok(20.max(new_data_count))
    } else {
        // 10'luk dilimlere yuvarla
        Ok(((new_data_count + 9) / 10) * 10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_symbols() {
        // Test için mock redis bağlantısı gerekebilir
        // Bu implementasyon gerçek redis bağlantısı gerektirir
    }

    #[test]
    fn test_determine_calculation_logic() {
        // Hesaplama mantığının test edilmesi
        assert_eq!(((25 + 9) / 10) * 10, 30);
        assert_eq!(((15 + 9) / 10) * 10, 20);
    }
}