extern crate influxdb2;

use pyo3::prelude::*;

#[pyclass(subclass)]
pub(crate) struct DB {
    pub(crate) client: influxdb2::Client,
}

#[pymethods]
impl DB {
    ///
    /// Instantiate a DB instance
    ///
    /// # Arguments
    ///
    /// * `host` - The host to connect to
    /// * `org` - The organization to read/write to
    /// * `token` - The token to use for authentication
    ///
    #[args(host, org, token)]
    #[new]
    pub fn new(host: &str, org: &str, token: &str) -> PyResult<Self> {
        let client = influxdb2::Client::new(host, org, token);

        Ok(DB { client })
    }

    /// Ping the database
    ///
    /// # Returns
    ///
    /// * `bool` - True if the database is reachable, false otherwise
    ///
    pub(crate) fn ping<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let client = self.client.clone();
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let ready = client.ready().await.expect("Failed to ping database");
                Python::with_gil(|py| Ok(ready.into_py(py)))
            },
        )
    }
}

#[pymodule]
fn rflux(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DB>()?;
    Ok(())
}
