extern crate influxdb2;

use std::collections::HashMap;

use futures::prelude::*;

use influxdb2::models::Buckets;
use influxdb2::Client;
use pyo3::exceptions::{PyConnectionError, PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};

#[derive(FromPyObject)]
#[pyclass]
pub struct PyEngine {
    host: String,
    token: String,
    org_id: String,
}

impl FieldType {
    fn extract_from_py_schema(prop: &PyAny) -> PyResult<Self> {
        let prop: &PyDict = prop.downcast()?;
        if let Some(data_type) = prop.get_item("type") {
            let data_type: &str = data_type.extract()?;

            //TODO
            match data_type {
                "array" => {
                    if let Some(items) = prop.get_item("items") {
                        match items.downcast::<PyList>() {
                            Ok(type_list) => {
                                let items = type_list
                                    .into_iter()
                                    .map(|v| Self::extract_from_py_schema(v))
                                    .collect::<PyResult<Vec<FieldType>>>()?;
                                Ok(Self::Tuple { items })
                            }
                            Err(_) => Ok(Self::List {
                                items: Box::new(Self::extract_from_py_schema(items)?),
                            }),
                        }
                    } else {
                        Ok(Self::List {
                            items: Box::new(Self::Str),
                        })
                    }
                }

                &_ => Ok(Self::Str),
            }
        } else {
            Ok(FieldType::Str)
        }
    }

    pub(crate) fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        match self {
            Self::Str => dict.set_item("type", "string")?,
            Self::List { items } => {
                dict.set_item("type", "array")?;
                dict.set_item("items", items.to_dict(py)?)?;
            }
            Self::Tuple { items } => {
                dict.set_item("type", "array")?;
                let list = PyList::empty(py);
                for item in items {
                    list.append(item.to_dict(py)?)?;
                }
                dict.set_item("items", list)?;
            }
        }
        Ok(dict.into())
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
    List { items: Box<FieldType> },
    Tuple { items: Vec<FieldType> },
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
                let prop: &PyDict = props.downcast()?;
                Schema::from_py_any(prop)
            } else {
                Err(PyValueError::new_err(
                    "Invalid schema. No 'properties' found",
                ))
            }
        })
    }

    pub(crate) fn from_py_any(props: &PyDict) -> PyResult<Self> {
        let props: &PyDict = props.downcast()?;
        let keys = props.keys();
        let mapping = keys
            .iter()
            .map(|key| {
                let value = props.get_item(key).unwrap();
                let key: String = key.extract()?;
                let value: FieldType = FieldType::extract_from_py_schema(value)?;
                Ok((key, value))
            })
            .collect::<PyResult<HashMap<String, FieldType>>>()?;
        Ok(Self { mapping })
    }

    pub(crate) fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);
        let props = PyDict::new(py);
        for (k, v) in &self.mapping {
            props.set_item(k, v.to_dict(py)?)?;
        }
        Ok(props.into())
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
impl _Bucket {
    pub(crate) fn new(name: String, meta: BucketMeta, client: influxdb2::Client) -> Self {
        Self { name, meta, client }
    }
}

#[pyclass(subclass)]
pub(crate) struct _Bucket {
    pub(crate) name: String,
    pub(crate) meta: BucketMeta,
    pub(crate) client: influxdb2::Client,
}

#[pymethods]
impl _Bucket {
    pub(crate) fn add<'b>(&self, py: Python<'b>, item: Py<PyAny>) -> PyResult<&'b PyAny> {
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
    pub(crate) fn to_dict(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let dict = PyDict::new(py);
            dict.set_item("name", self.name.clone())?;
            dict.set_item("meta", self.meta.to_dict(py)?)?;
            Ok(dict.into())
        })
    }
}

#[pyclass(subclass)]
pub(crate) struct _Registry {
    pub(crate) client: influxdb2::Client,
    buckets_meta: HashMap<String, BucketMeta>,
    model_type_map: HashMap<String, Py<PyType>>,
}

#[pyfunction]
pub fn create_engine(host: String, token: String, org_id: String) -> PyResult<PyEngine> {
    Ok(PyEngine {
        host,
        token,
        org_id,
    })
}

pub(crate) async fn list_buckets(client: &Client) -> Result<Buckets, influxdb2::RequestError> {
    client.list_buckets(None).await
}

#[pymethods]
impl _Registry {
    #[new]
    pub fn new(bind: PyEngine) -> PyResult<Self> {
        let client = influxdb2::Client::new(bind.host, bind.org_id, bind.token);
        Ok(_Registry {
            client,
            buckets_meta: Default::default(),
            model_type_map: Default::default(),
        })
    }

    pub(crate) fn _autoload(&mut self, base: Py<PyType>) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let buckets = base.getattr(py, "_get_collected_buckets")?.call0(py)?;

            let buckets = buckets.into_py(py);
            let ob: &PyDict = buckets.extract(py)?;
            if let Some(props) = ob.get_item("properties") {
                let buckets: &PyList = props.get_item("buckets").unwrap().extract()?;
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

    pub(crate) fn get_bucket(&mut self, model: Py<PyType>) -> PyResult<_Bucket> {
        let model_name: String = Python::with_gil(|py| model.getattr(py, "__name__")?.extract(py))?;

        if let Some(meta) = self.buckets_meta.get(&model_name) {
            Ok(_Bucket::new(
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

    pub(crate) fn get_buckets(&mut self) -> PyResult<Vec<_Bucket>> {
        Ok(self
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
        self.buckets_meta.insert(model_name.clone(), meta);
        self.model_type_map.insert(model_name.clone(), model);

        let client = self.client.clone();
        let bucket_options = Some(influxdb2::models::PostBucketRequest::new(
            self.client.org.clone(),
            model_name,
        ));
        pyo3_asyncio::tokio::future_into_py_with_locals(
            py,
            pyo3_asyncio::tokio::get_current_locals(py)?,
            async move {
                client
                    .create_bucket(bucket_options)
                    .await
                    .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;
                Python::with_gil(|py| Ok(true.into_py(py)))
            },
        )
    }

    pub(crate) fn delete_bucket<'a>(
        &mut self,
        py: Python<'a>,
        model: Py<PyType>,
    ) -> PyResult<&'a PyAny> {
        let model_name: String = Python::with_gil(|py| model.getattr(py, "__name__")?.extract(py))?;
        self.buckets_meta.remove(&model_name);
        self.model_type_map.remove(&model_name);

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
                Python::with_gil(|py| Ok(true.into_py(py)))
            },
        )
    }
}

#[pymodule]
fn adeline(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<_Registry>()?;
    m.add_class::<_Bucket>()?;
    m.add_function(wrap_pyfunction!(create_engine, m)?)?;
    Ok(())
}
