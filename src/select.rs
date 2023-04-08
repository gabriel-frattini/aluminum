use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;

#[pyclass(subclass)]
pub struct _Select {
    _select: Py<PyType>,
    _where_clauses: Vec<_WhereClause>,
    _raw_query: String,
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
                if let Ok(right_operand) = right_operand.parse::<i32>() {
                    self._raw_query.push_str(&format!(
                        "r.{} {} {})",
                        left_operand, operator, right_operand
                    ));
                    continue;
                }

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

#[derive(Clone)]
#[pyclass(subclass)]
pub struct _Mapped {
    pub _col_name: String,
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
