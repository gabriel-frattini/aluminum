use pyo3::prelude::*;

#[derive(FromPyObject)]
#[pyclass]
pub struct PyEngine {
    pub host: String,
    pub token: String,
    pub org_id: String,
}

#[pyfunction]
pub fn create_engine(host: String, token: String, org_id: String) -> PyResult<PyEngine> {
    Ok(PyEngine {
        host,
        token,
        org_id,
    })
}
