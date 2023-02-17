struct Point {
    name: String,
    latitude: f64,
    longitude: f64,
}

trait Distance {
    fn distance(&self, other: &Self) -> f64;
    fn duration(&self, other: &Self, speed: f64) -> f64;
}

impl Distance for Point {
    fn distance(&self, other: &Self) -> f64 {
        let d_lat = (other.latitude - self.latitude).to_radians();
        let d_lon = (other.longitude - self.longitude).to_radians();
        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();

        let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        let r = 6371.0; // Earth's radius in km

        r * c
    }

    fn duration(&self, other: &Self, speed: f64) -> f64 {
        self.distance(other) / speed
    }
}

fn main() {
    let p1 = Point {
        name: "Wellington".to_string(),
        latitude: -41.2923,
        longitude: 174.7788,
    };
    let p2 = Point {
        name: "Cologne".to_string(),
        latitude: 50.9375,
        longitude: 6.9603,
    };
    let speed = 80.0; // km/h
    println!(
        "The distance between {} and {} is {:.2} km.",
        p1.name,
        p2.name,
        p1.distance(&p2)
    );
    println!(
        "The time to travel from {} to {} at {:.0} km/h is {:.2} hours.",
        p1.name,
        p2.name,
        speed,
        p1.duration(&p2, speed)
    );
}
