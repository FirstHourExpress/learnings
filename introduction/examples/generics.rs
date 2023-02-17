use std::ops::Mul;

struct Measurement<T> {
    value: T,
    unit: String,
}

trait ConvertTo<T> {
    fn convert(&self) -> Measurement<T>;
}

impl<T: Mul<f64, Output = T> + Clone> ConvertTo<T> for Measurement<T> {
    fn convert(&self) -> Measurement<T> {
        let converted_value = self.value.clone().mul(2.54);
        Measurement {
            value: converted_value,
            unit: "cm".to_string(),
        }
    }
}

fn main() {
    let inches = Measurement {
        value: 10.0,
        unit: "inch".to_string(),
    };
    let centimeters: Measurement<f64> = inches.convert();
    println!(
        "{} {} is equal to {} {}.",
        inches.value, inches.unit, centimeters.value, centimeters.unit
    );
}
