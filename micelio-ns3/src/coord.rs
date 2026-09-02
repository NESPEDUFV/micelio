use std::{error::Error, io};

use kdtree::KdTree;

use crate::ffi;

pub struct CoordSpace {
    pub origin_lat: f64,
    pub origin_lng: f64,
    pub brite_size: f64,
    pub sim_width: f64,
    pub sim_height: f64,
    pub earth_radius: f64,
    pub tree: KdTree<f64, (u32, [f64; 2]), [f64; 2]>,
}

impl CoordSpace {
    pub fn new() -> Self {
        Self {
            origin_lat: 0.0,
            origin_lng: 0.0,
            brite_size: 5_000.0,
            sim_width: 5_000.0,
            sim_height: 5_000.0,
            earth_radius: 6_371_000.0,
            tree: KdTree::new(2),
        }
    }

    pub fn with_origin(mut self, lat: f64, lng: f64) -> Self {
        self.origin_lat = lat.to_radians();
        self.origin_lng = lng.to_radians();
        self
    }

    pub fn with_earth_radius(mut self, r: f64) -> Self {
        self.earth_radius = r;
        self
    }

    pub fn with_brite_size(mut self, s: f64) -> Self {
        self.brite_size = s;
        self
    }

    pub fn with_sim_size(mut self, w: f64, h: f64) -> Self {
        self.sim_width = w;
        self.sim_height = h;
        self
    }

    pub fn add_node(&mut self, id: u32, x: f64, y: f64) {
        let pos = self.brite_to_geo(x, y);
        self.tree
            .add(pos, (id, pos))
            .expect("should add node!")
    }

    pub fn nearest_node(&self, lat: f64, lng: f64) -> Result<ffi::BriteNodeResult, Box<dyn Error>> {
        let nearests = self
            .tree
            .nearest(&[lng, lat], 1, &micelio::fl::utils::haversine)?;
        let (distance, (id, pos)) = nearests
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("nearest not found"))?;
        Ok(ffi::BriteNodeResult {
            distance: 2.0 * distance.sqrt().asin() * self.earth_radius,
            id: *id,
            lat: pos[1],
            lng: pos[0],
        })
    }

    pub fn remove_node(&mut self, id: u32, lat: f64, lng: f64) -> Result<usize, kdtree::ErrorKind> {
        self.tree.remove(&[lng, lat], &(id, [lng, lat]))
    }

    pub fn euclid_to_geo(&self, x: f64, y: f64) -> [f64; 2] {
        let d_r = (x * x + y * y).sqrt() / self.earth_radius;
        let a = x.atan2(y);
        let sin_olat = self.origin_lat.sin();
        let cos_olat = self.origin_lat.cos();
        let sin_d_r = d_r.sin();
        let cos_d_r = d_r.cos();
        let lat = (sin_olat * cos_d_r + cos_olat * sin_d_r * a.cos()).asin();
        let lng =
            self.origin_lng + (a.sin() * sin_d_r * cos_olat).atan2(cos_d_r - sin_olat * lat.sin());
        [lng, lat]
    }

    pub fn geo_to_euclid(&self, lat: f64, lng: f64) -> [f64; 2] {
        let dlng = lng - self.origin_lng;
        let h = micelio::fl::utils::haversine(&[lng, lat], &[self.origin_lng, self.origin_lat]);
        let d = 2.0 * h.sqrt().asin() * self.earth_radius;
        let a = (dlng.sin() * lat.cos()).atan2(
            self.origin_lat.cos() * lat.sin() - self.origin_lat.sin() * lat.cos() * dlng.cos(),
        );
        let x = d * a.sin();
        let y = d * a.cos();
        [x, y]
    }

    pub fn brite_to_euclid(&self, x: f64, y: f64) -> [f64; 2] {
        let x_ = self.sim_width * (x / self.brite_size - 0.5);
        let y_ = self.sim_height * (y / self.brite_size - 0.5);
        [x_, -y_]
    }

    pub fn brite_to_geo(&self, x: f64, y: f64) -> [f64; 2] {
        let [x_, y_] = self.brite_to_euclid(x, y);
        self.euclid_to_geo(x_, y_)
    }
}
