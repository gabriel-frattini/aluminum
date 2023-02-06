extern crate influxdb2;

use std::collections::HashMap;

use futures::prelude::*;

use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};

fn extract_from_py_schema(prop: &PyAny) -> PyResult<FieldType> {
    let prop: &PyDict = prop.downcast()?;
    if let Some(data_type) = prop.get_item("type") {
        let data_type: &str = data_type.extract()?;

        //TODO
        match data_type {
            &_ => Ok(FieldType::Str),
        }
    } else {
        Ok(FieldType::Str)
    }
}

pub(crate) fn transform_point(
    schema: &Schema,
    obj: &Py<PyAny>,
) -> PyResult<(String, Vec<(String, String)>)> {
    let obj = Python::with_gil(|py| match obj.extract::<HashMap<String, Py<PyAny>>>(py) {
        Ok(v) => Ok(v),
        Err(_) => obj.getattr(py, "dict")?.call0(py)?.extract(py),
    })?;

    let mut records: Vec<(String, String)> = Vec::with_capacity(obj.len());
    let mut measurement = String::new();

    for (field, type_) in &schema.mapping {
        if let Some(v) = obj.get(field) {
            // TODO
            match type_ {
                _ => {
                    if field == "measurement" {
                        measurement = v.to_string();
                    }
                    records.push((field.clone(), v.to_string()));
                }
            };
        }
    }

    Ok((measurement, records))
}

#[derive(Clone, Debug)]
pub(crate) enum FieldType {
    Str,
}

#[derive(Clone, Debug)]
pub(crate) struct Schema {
    pub mapping: HashMap<String, FieldType>,
}

impl Schema {
    pub(crate) fn from_py_schema(ob: Py<PyAny>) -> PyResult<Self> {
        Python::with_gil(|py| {
            let ob = ob.into_py(py);
            let ob: &PyDict = ob.extract(py)?;
            if let Some(props) = ob.get_item("properties") {
                Schema::from_py_any(props)
            } else {
                Err(PyValueError::new_err(
                    "Invalid schema. No 'properties' found",
                ))
            }
        })
    }

    pub(crate) fn from_py_any(props: &PyAny) -> PyResult<Self> {
        let props: &PyDict = props.downcast()?;
        let keys = props.keys();
        let mapping = keys
            .iter()
            .map(|key| {
                let value = props.get_item(key).unwrap();
                let key: String = key.extract()?;
                let value: FieldType = extract_from_py_schema(value)?;
                Ok((key, value))
            })
            .collect::<PyResult<HashMap<String, FieldType>>>()?;
        Ok(Self { mapping })
    }
}

#[derive(Clone)]
#[pyclass(subclass)]
pub(crate) struct BucketMeta {
    pub(crate) schema: Box<Schema>,
}

impl BucketMeta {
    pub(crate) fn new(schema: Box<Schema>) -> Self {
        BucketMeta { schema }
    }
}
impl RFluxBucket {
    pub(crate) fn new(name: String, meta: BucketMeta, client: influxdb2::Client) -> Self {
        Self { name, meta, client }
    }
}

#[pyclass(subclass)]
pub(crate) struct RFluxBucket {
    pub(crate) name: String,
    pub(crate) meta: BucketMeta,
    pub(crate) client: influxdb2::Client,
}

#[pymethods]
impl RFluxBucket {
    pub(crate) fn insert<'b>(&self, py: Python<'b>, item: Py<PyAny>) -> PyResult<&'b PyAny> {
        let client = self.client.clone();
        let name = self.name.clone();
        let schema = self.meta.schema.clone();

        let records = transform_point(&schema, &item)?;
        let (measurement, records) = records;

        let mut point = influxdb2::models::data_point::DataPoint::builder(measurement);
        for (k, v) in records {
            if k == "field" {
                point = point.field(k.clone(), v.clone());
            } else if k == "tag" {
                point = point.tag(k.clone(), v.clone());
            }
        }
        let points = vec![point.build().unwrap()];

        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                client
                    .write(&name, stream::iter(points))
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                Python::with_gil(|py| Ok(true.into_py(py)))
            },
        )
    }
}

#[pyclass(subclass)]
pub(crate) struct RFlux {
    pub(crate) client: influxdb2::Client,
    buckets_meta: HashMap<String, BucketMeta>,
    model_type_map: HashMap<String, Py<PyType>>,
}

#[pymethods]
impl RFlux {
    #[args(host, org, token)]
    #[new]
    pub fn new(host: &str, org: String, token: String) -> PyResult<Self> {
        let client = influxdb2::Client::new(host, org, token);
        Ok(RFlux {
            client,
            buckets_meta: Default::default(),
            model_type_map: Default::default(),
        })
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
                    influxdb2::models::health::Status::Pass => true,
                    influxdb2::models::health::Status::Fail => false,
                };
                Python::with_gil(|py| Ok(healthy.into_py(py)))
            },
        )
    }

    pub(crate) fn get_bucket(&mut self, model: Py<PyType>) -> PyResult<RFluxBucket> {
        let model_name: String =
            Python::with_gil(|py| model.getattr(py, "__qualname__")?.extract(py))?;

        if let Some(meta) = self.buckets_meta.get(&model_name) {
            Ok(RFluxBucket::new(
                model_name,
                meta.clone(),
                self.client.clone(),
            ))
        } else {
            Err(PyKeyError::new_err(format!(
                "{} has not yet been created on the store",
                model_name
            )))
        }
    }

    pub(crate) fn create_bucket(&mut self, model: Py<PyType>) -> PyResult<RFluxBucket> {
        Python::with_gil(|py| {
            let schema = model.getattr(py, "schema")?.call0(py)?;
            let schema = Schema::from_py_schema(schema)?;
            let model_name: String =
                Python::with_gil(|py| model.getattr(py, "__qualname__")?.extract(py))?;

            let meta = BucketMeta::new(Box::new(schema));
            self.buckets_meta
                .insert(model_name.clone(), meta.clone());
            self.model_type_map.insert(model_name.clone(), model);
            Ok(RFluxBucket::new(model_name, meta, self.client.clone()))
        })
    }
}

#[pymodule]
fn rflux(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<RFlux>()?;
    m.add_class::<RFluxBucket>()?;
    Ok(())
}
