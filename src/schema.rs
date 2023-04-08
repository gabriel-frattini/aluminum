use std::collections::HashMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};

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
            .downcast::<PyAny>()?;

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
                                    .map(Self::extract_from_py_schema)
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
