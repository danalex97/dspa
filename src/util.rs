extern crate plotlib;

use plotlib::page::Page;
use plotlib::repr::Scatter;
use plotlib::style::{PointMarker, PointStyle};
use plotlib::view::ContinuousView;

pub struct Plotter {
    ctr: u32,
}

type Point = (f64, f64);

impl Plotter {
    pub fn new() -> Plotter {
        Plotter { ctr: 0 }
    }

    pub fn plot_points(
        &mut self,
        centers: &Vec<Point>,
        points: &Vec<Point>,
        outliers: &Vec<Point>,
    ) {
        let s1 = Scatter::from_slice(points).style(
            PointStyle::new()
                .marker(PointMarker::Square)
                .colour("#DD3355")
                .size(1.),
        );
        let s2 = Scatter::from_slice(outliers).style(PointStyle::new().colour("#442288").size(1.));
        let s3 = Scatter::from_slice(centers).style(PointStyle::new().colour("#35C788").size(2.));

        let v = ContinuousView::new()
            .add(&s1)
            .add(&s2)
            .add(&s3)
            .x_range(0., 1.)
            .y_range(0., 1.)
            .x_label("Some varying variable")
            .y_label("The response of something");

        // A page with a single view is then saved to an SVG file
        Page::single(&v)
            .save(format!("plots/scatter{}.svg", self.ctr))
            .unwrap();
        self.ctr += 1;
    }
}
