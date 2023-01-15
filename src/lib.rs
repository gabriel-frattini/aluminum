extern crate influxdb2;

use futures::prelude::*;

use pyo3::exceptions::PyConnectionError;
use pyo3::prelude::*;

#[pyclass(subclass)]
pub(crate) struct RFlux {
    pub(crate) client: influxdb2::Client,
}

#[pymethods]
impl RFlux {
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
    pub fn new(host: &str, org: String, token: String) -> PyResult<Self> {
        let client = influxdb2::Client::new(host, org, token);
        Ok(RFlux { client })
    }

    /// Checks database health
    ///
    /// # Returns
    ///
    /// * `bool` - True if the database is healthy, false otherwise
    ///
    pub(crate) fn healthy<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let client = self.client.clone();
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let status = client
                    .health()
                    .await
                    .map_err(|e| PyConnectionError::new_err(e.to_string()))?.status;

                let healthy = match status {
                    influxdb2::models::health::Status::Pass => true,
                    influxdb2::models::health::Status::Fail => false,
                };
                Python::with_gil(|py| Ok(healthy.into_py(py)))
            },
        )
    }

    /// Write a point to the database
    /// # Arguments
    /// * `measurement` - The measurement to write to
    /// * `fields` - The fields to write
    /// * `tags` - The tags to write
    /// * `timestamp` - The timestamp to write
    /// # Returns
    /// * `bool` - True if the write was successful, false otherwise
    ///
    pub(crate) fn write<'b>(
        &self,
        py: Python<'b>,
        bucket: String,
        measurement: String,
        field: String,
        tag: Option<String>,
        timestamp: Option<i64>,
    ) -> PyResult<&'b PyAny> {
        let client = self.client.clone();

        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let mut point =
                    influxdb2::models::DataPoint::builder(measurement).field(field, 1.0);
                if let Some(tag) = tag {
                    point = point.tag("tag", tag);
                }

                if let Some(timestamp) = timestamp {
                    point = point.timestamp(timestamp);
                }

                let points = vec![point.build().unwrap()];

                client
                    .write(&bucket, stream::iter(points))
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;

                Python::with_gil(|py| Ok(true.into_py(py)))
            },
        )
    }
}

#[pymodule]
fn rflux(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RFlux>()?;
    Ok(())
}
