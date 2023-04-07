extern crate influxdb2;

use std::collections::HashMap;
use std::hash::Hash;

use futures::prelude::*;

use influxdb2::models::data_point::DataPoint;
use influxdb2::models::health::Status;
use influxdb2::models::{PostBucketRequest, Query};
use influxdb2::Client;
use influxdb2::FromDataPoint;
use pyo3::exceptions::{self, PyConnectionError, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyDictItems, PyList, PyTuple, PyType};

#[derive(FromPyObject)]
#[pyclass]
pub struct PyEngine {
    host: String,
    token: String,
    org_id: String,
}

#[pyfunction]
pub fn get_schema(cls: &PyType) -> PyResult<PyObject> {
    Python::with_gil(|py| {
        let schema = PyDict::new(py);
        let title = cls.getattr("__name__");
        let title: &str = title.unwrap().extract().unwrap();
        schema.set_item("title", title)?;
        schema.set_item("type", "object")?;
        schema.set_item("required", PyList::empty(py))?;
        let schema_properties = PyDict::new(py);

        let items = cls
            .getattr("__annotations__")?
            .getattr("items")?
            .call0()?
            .downcast::<PyDictItems>()?;

        items
            .iter()?
            .collect::<PyResult<Vec<_>>>()?
            .iter()
            .try_for_each(|item| {
                let (key, value) = item.extract::<(&str, &PyAny)>().unwrap();
                let col_properties = PyDict::new(py);
                match value.getattr("__args__") {
                    Ok(v) => {
                        let v: &PyTuple = v.downcast()?;
                        let col_type = v[0].getattr("__name__")?;
                        let col_type: &str = col_type.extract()?;
                        col_properties.set_item("title", key)?;
                        col_properties.set_item("type", col_type)?;
                        schema_properties.set_item(key, col_properties)?;
                    }
                    Err(_) => {
                        col_properties.set_item("title", key)?;
                        let col_type = value.getattr("__class__")?.getattr("__name__")?;
                        let col_type: &str = col_type.extract()?;
                        col_properties.set_item("title", key)?;
                        col_properties.set_item("type", col_type)?;
                        schema_properties.set_item(key, col_properties)?;
                    }
                };

                Ok::<(), PyErr>(())
            })
            .unwrap();
        schema.set_item("properties", schema_properties)?;
        Ok(schema.into())
    })
}

#[pyclass(subclass)]
pub struct _Select {
    _select: Py<PyType>,
    _where_clauses: Vec<_WhereClause>,
    _raw_query: String,
}

#[derive(Clone)]
#[pyclass(subclass)]
pub struct _Mapped {
    _col_name: String,
}

#[pyclass(subclass)]
pub struct _WhereClause {
    _left_operand: _Mapped,
    _right_operand: Py<PyAny>,
    _operator: _WhereOperator,
}

#[pymethods]
impl _WhereClause {
    #[new]
    pub(crate) fn new(
        left_operand: _Mapped,
        right_operand: Py<PyAny>,
        operator: _WhereOperator,
    ) -> Self {
        Self {
            _left_operand: left_operand,
            _right_operand: right_operand,
            _operator: operator,
        }
    }

    pub(crate) fn __str__(&self) -> PyResult<String> {
        let left_operand = &self._left_operand._col_name;
        let right_operand = self._right_operand.to_string();
        let operator = match self._operator {
            _WhereOperator::EQ => "==",
            _WhereOperator::NE => "!=",
            _WhereOperator::GT => ">",
            _WhereOperator::GE => ">=",
            _WhereOperator::LT => "<",
            _WhereOperator::LE => "<=",
        };
        Ok(format!("{} {} {}", left_operand, operator, right_operand))
    }

    pub(crate) fn get_left_operand(&self) -> PyResult<_Mapped> {
        Ok(self._left_operand.clone())
    }

    pub(crate) fn get_right_operand(&self) -> PyResult<String> {
        Ok(self._right_operand.to_string())
    }

    pub(crate) fn get_operator_str(&self) -> PyResult<String> {
        let operator = match self._operator {
            _WhereOperator::EQ => "==",
            _WhereOperator::NE => "!=",
            _WhereOperator::GT => ">",
            _WhereOperator::GE => ">=",
            _WhereOperator::LT => "<",
            _WhereOperator::LE => "<=",
        };
        Ok(operator.to_string())
    }

    pub(crate) fn get_operator(&self) -> PyResult<_WhereOperator> {
        Ok(self._operator.clone())
    }
}

#[derive(Clone)]
#[pyclass]
pub enum _WhereOperator {
    EQ,
    NE,
    GT,
    GE,
    LT,
    LE,
}

#[pymethods]
impl _Mapped {
    #[new]
    pub(crate) fn new(col_name: String) -> Self {
        Self {
            _col_name: col_name,
        }
    }

    pub(crate) fn __eq__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::EQ,
        }
    }

    pub(crate) fn __ne__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::NE,
        }
    }

    pub(crate) fn __gt__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::GT,
        }
    }

    pub(crate) fn __ge__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::GE,
        }
    }

    pub(crate) fn __lt__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::LT,
        }
    }

    pub(crate) fn __le__(&mut self, value: Py<PyAny>) -> _WhereClause {
        _WhereClause {
            _left_operand: self.clone(),
            _right_operand: value,
            _operator: _WhereOperator::LE,
        }
    }

    pub(crate) fn _get_col_name(&self) -> PyResult<String> {
        Ok(self._col_name.clone())
    }
}

impl _WhereOperator {
    fn value(&self) -> &str {
        match *self {
            _WhereOperator::EQ => "==",
            _WhereOperator::NE => "!=",
            _WhereOperator::GT => ">",
            _WhereOperator::GE => ">=",
            _WhereOperator::LT => "<",
            _WhereOperator::LE => "<=",
        }
    }

    fn of(value: &str) -> PyResult<Self> {
        match value {
            "==" => Ok(_WhereOperator::EQ),
            "!=" => Ok(_WhereOperator::NE),
            ">" => Ok(_WhereOperator::GT),
            ">=" => Ok(_WhereOperator::GE),
            "<" => Ok(_WhereOperator::LT),
            "<=" => Ok(_WhereOperator::LE),
            _ => Err(PyValueError::new_err(format!(
                "Invalid operator: {}",
                value
            ))),
        }
    }
}

#[pymethods]
impl _Select {
    #[new]
    pub(crate) fn new(select: Py<PyType>) -> Self {
        Self {
            _select: select,
            _where_clauses: Vec::new(),
            _raw_query: String::new(),
        }
    }

    pub(crate) fn _where(
        &mut self,
        left_operand: _Mapped,
        right_operand: Py<PyAny>,
        operator: String,
    ) {
        self._where_clauses.push(_WhereClause {
            _left_operand: left_operand,
            _right_operand: right_operand,
            _operator: _WhereOperator::of(&operator).unwrap(),
        });
    }

    pub(crate) fn _create_bucket_str(&mut self, name: String) {
        self._raw_query = format!("from(bucket: \"{}\")", name);
    }

    pub(crate) fn _create_filter_str(&mut self) {
        if !self._where_clauses.is_empty() {
            for where_clause in self._where_clauses.iter() {
                self._raw_query.push_str(" |> filter(fn: (r) => ");
                let left_operand = &where_clause._left_operand._col_name;
                let right_operand = where_clause._right_operand.to_string();
                let operator: &str = where_clause._operator.value();
                self._raw_query.push_str(&format!(
                    "r.{} {} \"{}\")",
                    left_operand, operator, right_operand
                ));
            }
        }
    }

    pub(crate) fn _create_range_str(&mut self) {
        self._raw_query.push_str(" |> range(start: -1h)");
    }

    pub(crate) fn _create_raw_query(&mut self) {
        self._create_range_str();
        self._create_filter_str();
    }

    pub(crate) fn _get_raw_query(&self) -> PyResult<String> {
        Ok(self._raw_query.to_string())
    }
}

impl FieldType {
    fn extract_from_py_schema(prop: &PyAny) -> PyResult<Self> {
        let prop: &PyDict = prop.downcast()?;
        if let Some(data_type) = prop.get_item("type") {
            let data_type: &str = data_type.extract()?;

            match data_type {
                "null" => Ok(Self::None),
                "bool" => Ok(Self::Bool),
                "str" => Ok(Self::Str),
                "number" => Ok(Self::Float),
                "int" => Ok(Self::Int),
                "object" => Ok(Self::Dict {
                    value: Box::new(Self::Str),
                }),
                "list" => {
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
            Ok(Self::Str)
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
            Self::Dict { value } => {
                dict.set_item("type", "object")?;
                dict.set_item("properties", value.to_dict(py)?)?;
            }
            Self::Int => dict.set_item("type", "integer")?,
            Self::Float => dict.set_item("type", "number")?,
            Self::Bool => dict.set_item("type", "boolean")?,
            Self::None => dict.set_item("type", "null")?,
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

    for field in schema.mapping.keys() {
        if let Some(value) = obj.get(field) {
            if field == "measurement" {
                measurement = value.to_string();
            }
            records.push((field.clone(), value.to_string()));
        };
    }

    // E.g.:
    // ("test measurement", [("field", "10"), ("measurement", "test measurement"), ("tag", "test tag")])
    //
    Ok((measurement, records))
}

#[derive(Clone, Debug)]
pub(crate) enum FieldType {
    Dict { value: Box<FieldType> },
    List { items: Box<FieldType> },
    Tuple { items: Vec<FieldType> },
    Str,
    Int,
    Float,
    Bool,
    None,
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
                Schema::from_py_dict(prop)
            } else {
                Err(PyValueError::new_err(
                    "Invalid schema. No 'properties' found",
                ))
            }
        })
    }

    pub(crate) fn from_py_dict(props: &PyDict) -> PyResult<Self> {
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
    pub(crate) fn new(name: String, meta: BucketMeta, client: Client) -> Self {
        Self { name, meta, client }
    }
}

#[pyclass(subclass)]
pub(crate) struct _Bucket {
    pub(crate) name: String,
    pub(crate) meta: BucketMeta,
    pub(crate) client: Client,
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

#[pyclass(subclass)]
#[derive(Clone)]
pub(crate) struct _Registry {
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

#[pyclass(subclass)]
struct _Store {
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

#[pymodule]
fn aluminum(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<_Store>()?;
    m.add_class::<_Registry>()?;
    m.add_class::<_Select>()?;
    m.add_class::<_WhereClause>()?;
    m.add_class::<_WhereOperator>()?;
    m.add_class::<_Mapped>()?;
    m.add_class::<PyEngine>()?;
    m.add_class::<_Bucket>()?;
    m.add_function(wrap_pyfunction!(create_engine, m)?)?;
    m.add_function(wrap_pyfunction!(get_schema, m)?)?;
    Ok(())
}
