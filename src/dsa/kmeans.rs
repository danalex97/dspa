use rand::Rng;
use std::cmp::Ordering::Equal;

pub type Point = (f64, f64);
pub const EPS: f64 = 0.00001;

pub fn sqr_dist((x, y): &Point, (x2, y2): &Point) -> f64 {
    (x - x2) * (x - x2) + (y - y2) * (y - y2)
}

pub fn compute_clusters(centers: &Vec<Point>, points: &Vec<Point>) -> Vec<Vec<Point>> {
    let mut clusters = Vec::new();
    for _ in 0..centers.len() {
        clusters.push(vec![]);
    }
    for point in points {
        let mut i_closest = 0;
        for (i, center) in centers.iter().enumerate() {
            if sqr_dist(&center, &point) < sqr_dist(&centers[i_closest], &point) {
                i_closest = i;
            }
        }

        clusters[i_closest].push(*point);
    }
    clusters
}

pub fn compute_centers(
    old_centers: &Vec<Point>,
    points: &Vec<Point>,
    num_clusters: usize,
    min_coverage: usize,
) -> Vec<Point> {
    let mut centers = vec![];
    for center in old_centers.iter() {
        centers.push(*center);
    }

    // add new random centers
    while centers.len() < num_clusters {
        centers.push((
            rand::thread_rng().gen_range(0., 1.),
            rand::thread_rng().gen_range(0., 1.),
        ));
    }

    loop {
        let clusters = compute_clusters(&centers, &points);

        let mut new_centers = vec![];
        for cluster in clusters {
            let mut x_c = 0f64;
            let mut y_c = 0f64;
            for (x, y) in cluster.iter() {
                x_c += x;
                y_c += y;
            }
            x_c /= cluster.len() as f64;
            y_c /= cluster.len() as f64;
            new_centers.push((x_c, y_c));
        }

        let mut tot_diff = 0f64;
        for i in 0..new_centers.len() {
            tot_diff += sqr_dist(&new_centers[i], &centers[i]);
        }
        if tot_diff < EPS {
            break;
        }

        centers = new_centers;
        centers.retain(|(x, y)| !x.is_nan() && !y.is_nan());
    }

    // sort centers by coverage
    let mut ord: Vec<usize> = (0..centers.len()).collect();
    let clusters = compute_clusters(&centers, &points);
    ord.sort_by(|a, b| clusters[*a].len().cmp(&clusters[*b].len()));

    // remove clusters with small coverage
    let mut relevant_centers = vec![];
    for i in 0..centers.len() {
        if clusters[i].len() >= min_coverage {
            relevant_centers.push(centers[i]);
        }
    }

    relevant_centers
}

pub fn compute_outliers(centers: &Vec<Point>, points: &Vec<Point>) -> Vec<Point> {
    // calculate distances to closest cluster
    let mut dist = vec![];
    for point in points.iter() {
        let mut min_dist = 2.;
        for center in centers.iter() {
            if sqr_dist(center, point) < min_dist {
                min_dist = sqr_dist(center, point);
            }
        }
        dist.push(min_dist);
    }

    // compute percentile
    dist.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));
    let pct = (0.99 * dist.len() as f64) as usize;

    // find minimum distance for an outlier
    let mut dist_outlier = 2.;
    for i in pct..dist.len() - 1 {
        if dist[i + 1] - dist[i] > (dist[i] - dist[i - 1]) * 2. {
            dist_outlier = dist[i + 1];
            break;
        }
    }

    // find outliers
    let mut outliers = vec![];
    for point in points.iter() {
        let mut min_dist = 2.;
        for center in centers.iter() {
            if sqr_dist(center, point) < min_dist {
                min_dist = sqr_dist(center, point);
            }
        }

        if min_dist >= dist_outlier {
            outliers.push(*point);
        }
    }

    outliers
}
