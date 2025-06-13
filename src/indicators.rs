use yata::prelude::*;
use yata::core::{ValueType};
use yata::indicators::*;
use yata::methods::*;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub open_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub asset_volume: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorResult {
    pub kama21: Vec<f64>,
    pub kama34: Vec<f64>,
    pub kama21_roc1: Vec<f64>,
    pub cci34: Vec<f64>,
    pub cci170: Vec<f64>,
    pub atr14: Vec<f64>,
    pub donchian_upper: Vec<f64>,
    pub donchian_lower: Vec<f64>,
    pub ichimoku_conv: Vec<f64>,
    pub ichimoku_base: Vec<f64>,
    pub ichimoku_span_a: Vec<f64>,
    pub ichimoku_span_b: Vec<f64>,
    pub donc_position: Vec<f64>,
    pub kama_donc_position: Vec<f64>,
    pub close_kama_position: Vec<f64>,
}

impl From<&Candle> for yata::prelude::OHLCV {
    fn from(candle: &Candle) -> Self {
        yata::prelude::OHLCV {
            open: candle.open as ValueType,
            high: candle.high as ValueType,
            low: candle.low as ValueType,
            close: candle.close as ValueType,
            volume: candle.asset_volume as ValueType,
        }
    }
}

pub fn calculate_indicators(candles: &[Candle]) -> IndicatorResult {
    let len = candles.len();
    
    // OHLCV verilerini hazırla
    let ohlcv_data: Vec<yata::core::OHLCV> = candles.iter().map(|c| c.into()).collect();
    
    // Indikatörleri başlat
    let mut kama21 = KAMA::init(21, &KAMAConf::default()).unwrap();
    let mut kama34 = KAMA::new(34, &KAMAConf::default()).unwrap();
    let mut cci34 = CCI::new(34, &CCIConf::default()).unwrap();
    let mut cci170 = CCI::new(170, &CCIConf::default()).unwrap();
    let mut atr14 = ATR::new(14, &ATRConf::default()).unwrap();
    let mut dc = DonchianChannel::new(21, &DonchianChannelConf::default()).unwrap();
    let mut ichimoku = IchimokuCloud::new(9, 26, 52, &IchimokuCloudConf::default()).unwrap();
    
    // Sonuç vektörlerini başlat
    let mut kama21_values = Vec::with_capacity(len);
    let mut kama34_values = Vec::with_capacity(len);
    let mut cci34_values = Vec::with_capacity(len);
    let mut cci170_values = Vec::with_capacity(len);
    let mut atr14_values = Vec::with_capacity(len);
    let mut donchian_upper_values = Vec::with_capacity(len);
    let mut donchian_lower_values = Vec::with_capacity(len);
    let mut ichimoku_conv_values = Vec::with_capacity(len);
    let mut ichimoku_base_values = Vec::with_capacity(len);
    let mut ichimoku_span_a_values = Vec::with_capacity(len);
    let mut ichimoku_span_b_values = Vec::with_capacity(len);
    
    // Her OHLCV verisi için indikatörleri hesapla
    for ohlcv in &ohlcv_data {
        // KAMA hesaplamaları
        let kama21_result = kama21.next(ohlcv);
        let kama34_result = kama34.next(ohlcv);
        kama21_values.push(kama21_result as f64);
        kama34_values.push(kama34_result as f64);
        
        // CCI hesaplamaları
        let cci34_result = cci34.next(ohlcv);
        let cci170_result = cci170.next(ohlcv);
        cci34_values.push(cci34_result as f64);
        cci170_values.push(cci170_result as f64);
        
        // ATR hesaplaması
        let atr14_result = atr14.next(ohlcv);
        atr14_values.push(atr14_result as f64);
        
        // Donchian Channel hesaplaması
        let dc_result = dc.next(ohlcv);
        donchian_upper_values.push(dc_result.upper as f64);
        donchian_lower_values.push(dc_result.lower as f64);
        
        // Ichimoku hesaplaması
        let ichimoku_result = ichimoku.next(ohlcv);
        ichimoku_conv_values.push(ichimoku_result.conversion_line as f64);
        ichimoku_base_values.push(ichimoku_result.base_line as f64);
        ichimoku_span_a_values.push(ichimoku_result.leading_span_a as f64);
        ichimoku_span_b_values.push(ichimoku_result.leading_span_b as f64);
    }
    
    // KAMA ROC hesapla
    let mut kama21_roc1_values = Vec::with_capacity(len);
    for i in 0..len {
        if i == 0 {
            kama21_roc1_values.push(0.0);
        } else {
            let current = kama21_values[i];
            let previous = kama21_values[i - 1];
            if previous != 0.0 {
                let roc = (current - previous) / previous;
                kama21_roc1_values.push(roc);
            } else {
                kama21_roc1_values.push(0.0);
            }
        }
    }
    
    // Özel indikatörler
    let mut donc_position = vec![0.0; len];
    let mut kama_donc_position = vec![0.0; len];
    let mut close_kama_position = vec![0.0; len];

    for i in 0..len {
        if i < 21 { continue; } // Donchian için minimum 21 veri noktası gerekli
        
        let candle = &candles[i];
        let high = candle.high;
        let low = candle.low;
        let close = candle.close;
        let kama = kama21_values[i];
        let up = donchian_upper_values[i];
        let down = donchian_lower_values[i];
        let atr = atr14_values[i];

        // donc_price hesapla (yeşil/kırmızı mum kontrolü)
        let donc_price = if i > 0 && close >= candles[i-1].close { 
            high 
        } else { 
            low 
        };
        
        let range = up - down;
        if range != 0.0 {
            // DONC_POSITION: ((donc_price - donc_down) / (donc_up - donc_down)) - 0.5
            let dp = (donc_price - down) / range - 0.5;
            donc_position[i] = dp;
            
            // KAMA_DONC_POSITION: ((kama - donc_down) / (donc_up - donc_down)) - 0.5
            let kp = (kama - down) / range - 0.5;
            kama_donc_position[i] = kp;
            
            // CLOSE_KAMA_POSITION: (close - kama) / (2 * atr)
            if atr != 0.0 {
                let ckp = (close - kama) / (2.0 * atr);
                close_kama_position[i] = ckp;
            }
        }
    }

    IndicatorResult {
        kama21: kama21_values,
        kama34: kama34_values,
        kama21_roc1: kama21_roc1_values,
        cci34: cci34_values,
        cci170: cci170_values,
        atr14: atr14_values,
        donchian_upper: donchian_upper_values,
        donchian_lower: donchian_lower_values,
        ichimoku_conv: ichimoku_conv_values,
        ichimoku_base: ichimoku_base_values,
        ichimoku_span_a: ichimoku_span_a_values,
        ichimoku_span_b: ichimoku_span_b_values,
        donc_position,
        kama_donc_position,
        close_kama_position,
    }
}