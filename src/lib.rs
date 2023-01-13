extern crate influxdb2;

use futures::prelude::*;

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
    pub fn new(host: &str, org: String, token: String) -> PyResult<Self> {
        let client = influxdb2::Client::new(host, org, token);

        Ok(DB { client })
    }

    /// Ping the database
    ///
    /// # Returns
    ///
    /// * `bool` - True if the database is reachable, false otherwise
    ///
    /// We use lifetime elision here to avoid having to specify the lifetime of
    /// the returned string. This is safe because the returned string is
    /// guaranteed to live as long as the DB instance.
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
                    .expect("Failed to write point");
                Python::with_gil(|py| Ok(true.into_py(py)))
            },
        )
    }
}

#[pymodule]
fn rflux(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DB>()?;
    Ok(())
}
