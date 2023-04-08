use std::collections::HashMap;

use futures::prelude::*;
use influxdb2::models::data_point::DataPoint;
use influxdb2::models::Query;
use influxdb2::Client;
use influxdb2::FromDataPoint;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::schema::Schema;
#[pyclass(subclass)]
pub(crate) struct _Bucket {
    pub(crate) name: String,
    pub(crate) meta: BucketMeta,
    pub(crate) client: Client,
}
impl _Bucket {
    pub(crate) fn new(name: String, meta: BucketMeta, client: Client) -> Self {
        Self { name, meta, client }
    }
}

#[pymethods]
impl _Bucket {
    pub(crate) fn add<'b>(&self, py: Python<'b>, item: Py<PyAny>) -> PyResult<&'b PyAny> {
        let client = self.client.clone();
        let name = self.name.clone();
        let schema = self.meta.schema.clone();

        let records = transform_point(&schema, &item)?;
        let (measurement, records) = records;

        let mut point = DataPoint::builder(measurement);
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
    pub(crate) fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("name", self.name.clone())?;
            dict.set_item("meta", self.meta.to_dict(py)?)?;
            Ok(dict.into())
        })
    }

    pub(crate) fn raw_query<'a>(&self, query: String, py: Python<'a>) -> PyResult<&'a PyAny> {
        let client = self.client.clone();
        let name = self.name.clone();

        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                let query = Query::new(query);
                let stream: Vec<QueryResult> = client
                    .query::<QueryResult>(Some(query))
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;

                Python::with_gil(|py| {
                    let result = PyDict::new(py);
                    result.set_item("name", name)?;
                    let list = PyList::empty(py);
                    for item in stream {
                        let dict = PyDict::new(py);
                        dict.set_item("measurement", item.measurement)?;
                        dict.set_item("tag", item.tag)?;
                        dict.set_item("field", item.field.parse::<i32>().unwrap())?;
                        list.append(dict)?;
                    }
                    result.set_item("data", list)?;

                    Ok(Into::<PyObject>::into(result))
                })
            },
        )
    }
}

#[derive(Clone, Debug)]
#[pyclass(subclass)]
pub(crate) struct BucketMeta {
    pub(crate) schema: Box<Schema>,
}

impl BucketMeta {
    pub(crate) fn new(schema: Box<Schema>) -> Self {
        BucketMeta { schema }
    }

    pub(crate) fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        dict.set_item("schema", self.schema.to_dict(py)?)?;
        Ok(dict.into())
    }
}

#[derive(FromDataPoint, Debug)]
pub struct QueryResult {
    pub measurement: String,
    pub tag: String,
    pub field: String,
}

impl Default for QueryResult {
    fn default() -> Self {
        Self {
            measurement: String::new(),
            tag: String::new(),
            field: String::new(),
        }
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

    for field in schema.mapping.keys() {
        if let Some(value) = obj.get(field) {
            if field == "measurement" {
                measurement = value.to_string();
            }
            records.push((field.clone(), value.to_string()));
        };
    }

    Ok((measurement, records))
}
