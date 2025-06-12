use yata::indicators::*;
use yata::core::ValueType;
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

pub fn calculate_indicators(candles: &[Candle]) -> IndicatorResult {
    let len = candles.len();
    let closes: Vec<ValueType> = candles.iter().map(|c| c.close as ValueType).collect();
    let highs: Vec<ValueType> = candles.iter().map(|c| c.high as ValueType).collect();
    let lows: Vec<ValueType> = candles.iter().map(|c| c.low as ValueType).collect();

    let kama21 = KAMA::new(21, &closes).ma;
    let kama34 = KAMA::new(34, &closes).ma;
    let kama21_roc1 = ROC::new(1, &kama21).roc;

    let cci34 = CCI::new(34, &highs, &lows, &closes).cci;
    let cci170 = CCI::new(170, &highs, &lows, &closes).cci;

    let atr14 = ATR::new(14, &highs, &lows, &closes).atr;

    let dc = DonchianChannel::new(21, &highs, &lows);
    let donchian_upper = dc.upper;
    let donchian_lower = dc.lower;

    let ichi = IchimokuCloud::default().calculate(&highs, &lows, &closes);
    let ichimoku_conv = ichi.0;
    let ichimoku_base = ichi.1;
    let ichimoku_span_a = ichi.2;
    let ichimoku_span_b = ichi.3;

    // Özel göstergeler
    let mut donc_position = vec![0.0; len];
    let mut kama_donc_position = vec![0.0; len];
    let mut close_kama_position = vec![0.0; len];

    for i in 0..len {
        if i < 21 { continue; }
        let high = candles[i].high;
        let low = candles[i].low;
        let close = candles[i].close;
        let kama = kama21[i];
        let up = donchian_upper[i];
        let down = donchian_lower[i];
        let atr = atr14[i];

        let donc_price = if close >= candles[i-1].close { high } else { low };
        let range = up - down;
        if range != 0.0 {
            let dp = (donc_price - down) / range - 0.5;
            let kp = (kama - down) / range - 0.5;
            let ckp = (close - kama) / (2.0 * atr);
            donc_position[i] = dp;
            kama_donc_position[i] = kp;
            close_kama_position[i] = ckp;
        }
    }

    IndicatorResult {
        kama21,
        kama34,
        kama21_roc1,
        cci34,
        cci170,
        atr14,
        donchian_upper,
        donchian_lower,
        ichimoku_conv,
        ichimoku_base,
        ichimoku_span_a,
        ichimoku_span_b,
        donc_position,
        kama_donc_position,
        close_kama_position,
    }
}
