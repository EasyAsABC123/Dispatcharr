//! CharField rating conversion for the Source XC list projection.
//! Two actual Source TCP vectors are retained separately; other grammar and
//! rounding controls are definition-derived until a broader Source gate.
use dispatcharr_ingest_core::source_float_literal;
use dispatcharr_store::source_finite_float_string;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Error {
    SourceValue,
    Nonfinite,
    Capacity,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Rating {
    Empty,
    Finite(String),
}
/// Source rating is nullable CharField(max_length=10), never SQL NUMERIC.
/// The caller retains the SQL string and output grant through this operation.
pub(crate) fn half(raw: Option<&str>) -> Result<Rating, Error> {
    let Some(raw) = raw.filter(|text| !text.is_empty()) else {
        return Ok(Rating::Empty);
    };
    if raw.len() > 40 || raw.chars().count() > 10 {
        return Err(Error::Capacity);
    }
    Ok(Rating::Finite(half_number(
        source_float_literal(raw).ok_or(Error::SourceValue)?,
    )?))
}
/// Same finite two-decimal rounding for info's typed detailed-info overrides.
/// CharField length checks belong only to half(), not arbitrary JSON numbers.
pub(crate) fn half_number(value: f64) -> Result<String, Error> {
    let value = value / 2.0;
    if !value.is_finite() {
        return Err(Error::Nonfinite);
    }
    let fixed = format!("{value:.2}");
    if fixed.len() > 314 {
        return Err(Error::Capacity);
    }
    let rounded = fixed.parse::<f64>().map_err(|_| Error::SourceValue)?;
    source_finite_float_string(rounded).ok_or(Error::Nonfinite)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_tcp38_movie_and_series_vectors() {
        assert_eq!(half(Some("7.50")), Ok(Rating::Finite("3.75".into())));
        assert_eq!(half(Some("8.25")), Ok(Rating::Finite("4.12".into())));
    }
    #[test]
    fn absent_is_distinct_from_truthy_zero_and_negative_zero() {
        assert_eq!(half(None), Ok(Rating::Empty));
        assert_eq!(half(Some("")), Ok(Rating::Empty));
        assert_eq!(half(Some("0")), Ok(Rating::Finite("0.0".into())));
        assert_eq!(half(Some("-0")), Ok(Rating::Finite("-0.0".into())));
    }
    #[test]
    fn no_nonfinite_panic_or_silent_zero() {
        for value in ["NaN", "inf", "-Infinity", "1e9999"] {
            assert_eq!(half(Some(value)), Err(Error::Nonfinite));
        }
        assert_eq!(half(Some("not-float")), Err(Error::SourceValue));
        assert_eq!(half(Some("12345678901")), Err(Error::Capacity));
    }
}
