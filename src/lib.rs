extern crate influxdb;
use pyo3::prelude::*;

#[pyclass(subclass)]
pub(crate) struct DB {
    client: influxdb::Client,
}

#[pymethods]
impl DB {
    ///
    /// Instantiate a DB isntance
    ///
    /// # Arguments
    ///
    /// * `url` - Connection url
    /// * `name` - Database name
    ///
    #[args(url, name)]
    #[new]
    pub fn new(url: &str, name: &str) -> PyResult<Self> {
        let client = influxdb::Client::new(url, name);

        Ok(DB { client })
    }
}

#[pymodule]
fn rflux(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DB>()?;
    Ok(())
}
