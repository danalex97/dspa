use rand::Rng;
use std::cmp::Ordering::Equal;
use std::f64;

pub type Point = (f64, f64);
pub const EPS: f64 = 0.00001;
pub const SQR_DIST_FACTOR: f64 = 2.;

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

pub fn compute_outliers(centers: &Vec<Point>, points: &Vec<Point>, percentile: f64) -> Vec<Point> {
    // calculate distances to closest cluster
    let mut dist = vec![];
    for point in points.iter() {
        let mut min_dist = f64::INFINITY;
        for center in centers.iter() {
            if sqr_dist(center, point) < min_dist {
                min_dist = sqr_dist(center, point);
            }
        }
        dist.push(min_dist);
    }

    // compute percentile
    dist.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));
    let pct = (percentile * dist.len() as f64) as usize;

    // find minimum distance for an outlier
    let mut dist_outlier = f64::INFINITY;
    for i in pct..dist.len() - 1 {
        if dist[i + 1] - dist[i] > (dist[i] - dist[i - 1]) * SQR_DIST_FACTOR {
            dist_outlier = dist[i + 1];
            break;
        }
    }

    // find outliers
    let mut outliers = vec![];
    for point in points.iter() {
        let mut min_dist = f64::INFINITY;
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

#[cfg(test)]
mod kmeans_tests {
    use crate::dsa::kmeans::*;

    #[test]
    fn test_custers_computed_correctly() {
        let points = vec![
            (0., 0.),
            (1., 0.),
            (0., 1.),
            (1., 1.),
            (5., 5.),
            (5., 6.),
            (6., 5.),
            (6., 6.),
        ];

        let centers = vec![(0.5, 0.5), (5.5, 5.5)];

        assert_eq!(
            compute_clusters(&centers, &points)[0],
            vec![(0., 0.), (1., 0.), (0., 1.), (1., 1.),]
        );
        assert_eq!(
            compute_clusters(&centers, &points)[1],
            vec![(5., 5.), (5., 6.), (6., 5.), (6., 6.),]
        );
    }

    #[test]
    fn test_centers_computed_correctly() {
        let points = vec![
            (0., 0.),
            (1., 0.),
            (0., 1.),
            (1., 1.),
            (5., 5.),
            (5., 6.),
            (6., 5.),
            (6., 6.),
        ];
        let mut centers = compute_centers(&vec![], &points, 2, 4);
        centers.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Equal));

        assert_eq!(centers, vec![(0.5, 0.5), (5.5, 5.5),],);
    }

    #[test]
    fn test_centers_without_coverage_eliminated() {
        let points = vec![
            (0., 0.),
            (1., 0.),
            (0., 1.),
            (1., 1.),
            (5., 5.),
            (5., 6.),
            (6., 5.),
            (6., 6.),
        ];

        assert_eq!(compute_centers(&vec![], &points, 2, 5), vec![]);
    }

    #[test]
    fn test_outliers_computed_correctly() {
        let points = vec![
            (0., 0.),
            (1., 0.),
            (0., 1.),
            (1., 1.),
            (5., 5.),
            (5., 6.),
            (6., 5.),
            (6., 6.),
            (7.5, 7.5),
            (-10., -2.),
        ];

        let centers = vec![(0.5, 0.5), (5.5, 5.5)];

        assert_eq!(compute_outliers(&centers, &points, 0.8), vec![(-10., -2.),]);
    }
}
