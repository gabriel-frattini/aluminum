use std::collections::HashMap;

use influxdb2::models::{PostBucketRequest, Status};
use influxdb2::Client;
use pyo3::exceptions::{PyConnectionError, PyKeyError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};

use crate::bucket::{BucketMeta, _Bucket};
use crate::engine::PyEngine;
use crate::schema::Schema;

#[pyclass(subclass)]
pub struct _Store {
    client: Client,
    registry: _Registry,
}

#[pymethods]
impl _Store {
    #[new]
    pub fn new(bind: PyEngine, registry: _Registry) -> PyResult<Self> {
        let client = Client::new(&bind.host, &bind.org_id, &bind.token);
        Ok(_Store { client, registry })
    }

    pub(crate) fn healthy<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let client = self.client.clone();
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let status = client
                    .health()
                    .await
                    .map_err(|e| PyConnectionError::new_err(e.to_string()))?
                    .status;

                let healthy = match status {
                    Status::Pass => true,
                    Status::Fail => false,
                };
                Python::with_gil(|py| Ok(healthy.into_py(py)))
            },
        )
    }

    pub(crate) fn get_bucket(&mut self, model: Py<PyType>) -> PyResult<_Bucket> {
        let model_name: String = Python::with_gil(|py| model.getattr(py, "__name__")?.extract(py))?;

        if let Some(meta) = self.registry.buckets_meta.get(&model_name) {
            Ok(_Bucket::new(model_name, meta.clone(), self.client.clone()))
        } else {
            Err(PyKeyError::new_err(format!(
                "{} does not exist in the registry",
                model_name
            )))
        }
    }

    pub(crate) fn get_buckets(&mut self) -> PyResult<Vec<_Bucket>> {
        Ok(self
            .registry
            .buckets_meta
            .iter()
            .map(|(name, meta)| _Bucket::new(name.clone(), meta.clone(), self.client.clone()))
            .collect())
    }

    pub(crate) fn create_bucket<'a>(
        &mut self,
        model: Py<PyType>,
        py: Python<'a>,
    ) -> PyResult<&'a PyAny> {
        let schema = Python::with_gil(|py| model.getattr(py, "schema")?.call0(py))?;
        let schema = Schema::from_py_schema(schema)?;
        let model_name: String = Python::with_gil(|py| model.getattr(py, "__name__")?.extract(py))?;

        let meta = BucketMeta::new(Box::new(schema));
        self.registry.buckets_meta.insert(model_name.clone(), meta);
        self.registry
            .model_type_map
            .insert(model_name.clone(), model);

        let client = self.client.clone();
        let bucket_options = Some(PostBucketRequest::new(self.client.org.clone(), model_name));
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                client
                    .create_bucket(bucket_options)
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                Python::with_gil(|py| Ok(py.None()))
            },
        )
    }

    pub(crate) fn delete_bucket<'a>(
        &mut self,
        py: Python<'a>,
        model: Py<PyType>,
    ) -> PyResult<&'a PyAny> {
        let model_name: String = Python::with_gil(|py| model.getattr(py, "__name__")?.extract(py))?;
        self.registry.buckets_meta.remove(&model_name);
        self.registry.model_type_map.remove(&model_name);

        let client = self.client.clone();
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let buckets = client
                    .list_buckets(None)
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;

                let bucket = buckets.buckets.into_iter().find(|b| b.name == model_name);

                if let Some(bucket) = bucket {
                    if let Some(bucket_id) = bucket.id {
                        client.delete_bucket(&bucket_id).await.map_err(|e| {
                            pyo3::exceptions::PyConnectionError::new_err(e.to_string())
                        })?;
                    }
                };
                Python::with_gil(|py| Ok(py.None()))
            },
        )
    }
}

#[pyclass(subclass)]
#[derive(Clone)]
pub struct _Registry {
    buckets_meta: HashMap<String, BucketMeta>,
    model_type_map: HashMap<String, Py<PyType>>,
}

#[pymethods]
impl _Registry {
    #[new]
    pub fn new() -> PyResult<Self> {
        Ok(_Registry {
            buckets_meta: Default::default(),
            model_type_map: Default::default(),
        })
    }

    pub(crate) fn _autoload(&mut self, base: Py<PyType>) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let buckets = base.getattr(py, "_get_collected_buckets")?.call0(py)?;

            let buckets = buckets.into_py(py);
            let ob: &PyDict = buckets.extract(py)?;
            if let Some(props) = ob.get_item("buckets") {
                let buckets: &PyList = props.extract()?;
                for b in buckets.iter() {
                    let schema = b.getattr("schema")?.call0()?;
                    let model_name: String = schema.get_item("title").unwrap().extract()?;

                    let schema = schema.into_py(py);
                    let py_schema = Schema::from_py_schema(schema)?;

                    let meta = BucketMeta::new(Box::new(py_schema.clone()));
                    self.buckets_meta.insert(model_name.clone(), meta.clone());
                    let py_type = b.get_type().into_py(py);
                    self.model_type_map.insert(model_name.clone(), py_type);
                }
            }
            Ok(py.None())
        })
    }
}
