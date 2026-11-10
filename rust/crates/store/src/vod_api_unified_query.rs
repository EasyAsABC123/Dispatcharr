//! Source-derived unified QueryDict/access/filter policies.
//! Actual Source query/SQL/HTTP component proofs are scoped separately from
//! canonical app, React, browsable HTML and uncaptured grammar contracts.
//! Original Dispatcharr authors/contributors retain application credit/AGPL.
use super::{
    vod_api_pagination::last_query,
    vod_api_projection::{category_filter, Visibility},
};
use num_bigint::BigInt;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UnifiedPage {
    pub number: BigInt,
    pub size: BigInt,
    pub offset: BigInt,
}
#[derive(Clone, Debug)]
pub(super) struct UnifiedQuery {
    pub page: UnifiedPage,
    pub search: String,
    pub category: String,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum QueryFault {
    View(String),
    Capacity,
    Uncaptured(&'static str),
}
fn integer(raw: &str) -> Result<BigInt, QueryFault> {
    if let Some(value) =
        dispatcharr_ingest_core::source_integer_literal(raw).and_then(|v| v.parse().ok())
    {
        return Ok(value);
    }
    if raw.chars().count() > 200 {
        return Err(QueryFault::Uncaptured(
            "Long invalid Python integer error grammar needs Source capture",
        ));
    }
    let quoted = crate::python_action::collection_text(&Value::String(raw.into()), 65536)
        .ok_or(QueryFault::Capacity)?;
    Err(QueryFault::View(format!(
        "invalid literal for int() with base 10: {quoted}"
    )))
}
impl UnifiedQuery {
    /// Both access flags disabled returns before parsing in the actual view.
    /// Size parses before number, unlike the older unused pagination prototype.
    pub fn parse(
        pairs: &[(String, String)],
        visible: &Visibility,
    ) -> Result<Option<Self>, QueryFault> {
        if !visible.movies && !visible.series {
            return Ok(None);
        }
        let size = integer(last_query(pairs, "page_size").unwrap_or("24"))?;
        let number = integer(last_query(pairs, "page").unwrap_or("1"))?;
        let offset = (&number - BigInt::from(1)) * &size;
        Ok(Some(Self {
            page: UnifiedPage {
                number,
                size,
                offset,
            },
            search: last_query(pairs, "search").unwrap_or("").into(),
            category: last_query(pairs, "category").unwrap_or("").into(),
        }))
    }
    /// Preserve independent active-provider and category witnesses. A matching
    /// category need not belong to the same provider proving active membership.
    /// No ordinary hide-adult, account-type, enabled-link or year/order filter.
    pub fn predicates(&self, visible: &Visibility) -> (String, Vec<String>, String, Vec<String>) {
        let mut movie="movies.id IN (SELECT DISTINCT movie_id FROM vod_m3umovierelation mmr JOIN m3u_m3uaccount ma ON mmr.m3u_account_id = ma.id WHERE ma.is_active = true)".to_owned();
        let mut series="series.id IN (SELECT DISTINCT series_id FROM vod_m3useriesrelation msr JOIN m3u_m3uaccount ma ON msr.m3u_account_id = ma.id WHERE ma.is_active = true)".to_owned();
        let (mut mp, mut sp) = (Vec::new(), Vec::new());
        if !visible.movies {
            movie = "1=0".into();
        }
        if !visible.series {
            series = "1=0".into();
        }
        if !self.search.is_empty() {
            let pattern = format!("%{}%", crate::username::lower_source(&self.search));
            if visible.movies {
                movie.push_str(" AND LOWER(movies.name) LIKE ?");
                mp.push(pattern.clone());
            }
            if visible.series {
                series.push_str(" AND LOWER(series.name) LIKE ?");
                sp.push(pattern);
            }
        }
        if !self.category.is_empty() {
            let category = category_filter(&self.category);
            let m=" AND movies.id IN (SELECT movie_id FROM vod_m3umovierelation mmr JOIN vod_vodcategory c ON mmr.category_id = c.id WHERE c.name = ?)";
            let s=" AND series.id IN (SELECT series_id FROM vod_m3useriesrelation msr JOIN vod_vodcategory c ON msr.category_id = c.id WHERE c.name = ?)";
            match category.category_type {
                Some("movie") => {
                    if visible.movies {
                        movie.push_str(m);
                        mp.push(category.name.into());
                    } else {
                        movie = "1=0".into();
                    }
                    series = "1=0".into();
                    sp.clear();
                }
                Some("series") => {
                    if visible.series {
                        series.push_str(s);
                        sp.push(category.name.into());
                    } else {
                        series = "1=0".into();
                    }
                    movie = "1=0".into();
                    mp.clear();
                }
                _ => {
                    if visible.movies {
                        movie.push_str(m);
                        mp.push(category.name.into());
                    }
                    if visible.series {
                        series.push_str(s);
                        sp.push(category.name.into());
                    }
                }
            }
        }
        (movie, mp, series, sp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn actual_source_querydict_integer_and_disabled_branch_vectors() {
        let corpus: Value =
            serde_json::from_str(include_str!("../datasets/vod-unified-query-source-3.json"))
                .unwrap();
        let mut compared = 0;
        for row in corpus["records"].as_array().unwrap() {
            let role = row["role"].as_str().unwrap();
            let properties = match role {
                "movies_off" | "admin_movies_off" => {
                    serde_json::json!({"vod_movies_enabled":false})
                }
                "series_off" => serde_json::json!({"vod_series_enabled":false}),
                "both_off" => {
                    serde_json::json!({"vod_movies_enabled":false,"vod_series_enabled":false})
                }
                "zero_flags" => serde_json::json!({"vod_movies_enabled":0,"vod_series_enabled":0}),
                _ => serde_json::json!({}),
            };
            let visible = super::super::vod_api_projection::visibility(&properties, 1).unwrap();
            let uri = row["path"].as_str().unwrap();
            let pairs =
                url::form_urlencoded::parse(uri.split_once('?').map_or("", |v| v.1).as_bytes())
                    .map(|(a, b)| (a.into_owned(), b.into_owned()))
                    .collect::<Vec<_>>();
            let parsed = UnifiedQuery::parse(&pairs, &visible);
            if role == "both_off" {
                assert!(parsed.unwrap().is_none());
                compared += 1;
                continue;
            }
            if row["source_view_error"]
                .as_str()
                .is_some_and(|v| v.starts_with("invalid literal for int()"))
            {
                assert_eq!(
                    parsed.unwrap_err(),
                    QueryFault::View(row["source_view_error"].as_str().unwrap().into())
                );
                compared += 1;
                continue;
            }
            let parsed = parsed.unwrap().unwrap();
            if let Some(limit) = row["source_integer_limit"].as_str() {
                assert_eq!(parsed.page.size.to_string(), limit, "{uri}");
                assert_eq!(
                    parsed.page.offset.to_string(),
                    row["source_integer_offset"].as_str().unwrap(),
                    "{uri}"
                );
            }
            if row["source_view_error"].as_str()
                == Some("PostgreSQL text fields cannot contain NUL (0x00) bytes")
            {
                assert!(
                    matches!(parsed.page(&visible,128*1024),Err(QueryFault::View(message))if message==row["source_view_error"].as_str().unwrap())
                );
            }
            compared += 1;
        }
        assert!(
            compared >= 70,
            "Derived Source fixture unexpectedly missing cases"
        );
    }
}
