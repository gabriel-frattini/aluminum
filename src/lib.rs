extern crate influxdb2;

mod bucket;
mod engine;
mod schema;
mod select;
mod store;

use pyo3::prelude::*;
use schema::get_schema;
use select::_Select;

use self::bucket::_Bucket;
use self::engine::{create_engine, PyEngine};
use self::select::{_Mapped, _WhereClause, _WhereOperator};
use self::store::{_Registry, _Store};

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
