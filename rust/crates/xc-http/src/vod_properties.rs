//! Fixed borrowed property fields; unknown properties remain in charged SQL text.
//! Internal lexical spans do not enable shared serde_json features. Original
//! Dispatcharr authors retain DTO/application credit.
use crate::{
    vod_json::{self, Error},
    vod_span::{self, RawValue},
};
fn empty() -> &'static RawValue {
    "\"\""
}
fn array() -> &'static RawValue {
    "[]"
}
fn object() -> &'static RawValue {
    "{}"
}
fn false_value() -> &'static RawValue {
    "false"
}
#[derive(Default)]
struct Present<'a>(Option<&'a RawValue>);
struct Fields<'a> {
    movie_image: &'a RawValue,
    backdrop_path: &'a RawValue,
    youtube_trailer: &'a RawValue,
    director: &'a RawValue,
    actors: &'a RawValue,
    cast: &'a RawValue,
    release_date: Present<'a>,
    episode_run_time: &'a RawValue,
    detailed_fetched: &'a RawValue,
    episodes_fetched: &'a RawValue,
    detailed_info: &'a RawValue,
    basic_data: &'a RawValue,
}
impl Default for Fields<'_> {
    fn default() -> Self {
        Self {
            movie_image: empty(),
            backdrop_path: array(),
            youtube_trailer: empty(),
            director: empty(),
            actors: empty(),
            cast: empty(),
            release_date: Present::default(),
            episode_run_time: empty(),
            detailed_fetched: false_value(),
            episodes_fetched: false_value(),
            detailed_info: object(),
            basic_data: object(),
        }
    }
}
#[derive(Clone, Copy)]
pub(crate) enum Field {
    MovieImage,
    Backdrop,
    Trailer,
    Director,
    Actors,
    Cast,
    ReleaseDate,
    EpisodeRunTime,
    #[cfg_attr(not(test), allow(dead_code))] // Used only by the still-gated info DTOs.
    DetailedFetched,
    #[cfg_attr(not(test), allow(dead_code))] // Used only by the still-gated info DTOs.
    EpisodesFetched,
    #[cfg_attr(not(test), allow(dead_code))] // Used only by the still-gated info DTOs.
    DetailedInfo,
    #[cfg_attr(not(test), allow(dead_code))] // Used only by the still-gated info DTOs.
    BasicData,
}
enum Shape<'a> {
    Mapping(Fields<'a>),
    NonMapping,
}
pub(crate) struct Properties<'a>(Shape<'a>);
impl<'a> Properties<'a> {
    /// Input remains borrowed from charged SQL text. The caller checks the
    /// conservative scratch quote before this call; no new permit is acquired.
    pub fn parse(text: Option<&'a str>) -> Result<Self, Error> {
        let Some(text) = text else {
            return Ok(Self(Shape::Mapping(Fields::default())));
        };
        let raw = vod_span::parse(text)?;
        vod_json::validate_source_integer_limits(raw)?;
        if raw.starts_with('{') {
            let mut fields = Fields::default();
            for member in vod_span::Object::new(raw)? {
                let (key, value) = member?;
                match key.as_ref() {
                    "movie_image" => fields.movie_image = value,
                    "backdrop_path" => fields.backdrop_path = value,
                    "youtube_trailer" => fields.youtube_trailer = value,
                    "director" => fields.director = value,
                    "actors" => fields.actors = value,
                    "cast" => fields.cast = value,
                    "release_date" => fields.release_date = Present(Some(value)),
                    "episode_run_time" => fields.episode_run_time = value,
                    "detailed_fetched" => fields.detailed_fetched = value,
                    "episodes_fetched" => fields.episodes_fetched = value,
                    "detailed_info" => fields.detailed_info = value,
                    "basic_data" => fields.basic_data = value,
                    _ => {}
                }
            }
            Ok(Self(Shape::Mapping(fields)))
        } else if !vod_json::truthy(raw)? {
            Ok(Self(Shape::Mapping(Fields::default())))
        } else {
            Ok(Self(Shape::NonMapping))
        }
    }
    pub fn get(&self, key: Field) -> Result<&'a RawValue, Error> {
        let Shape::Mapping(fields) = &self.0 else {
            return Err(Error::SourceObject);
        };
        Ok(match key {
            Field::MovieImage => fields.movie_image,
            Field::Backdrop => fields.backdrop_path,
            Field::Trailer => fields.youtube_trailer,
            Field::Director => fields.director,
            Field::Actors => fields.actors,
            Field::Cast => fields.cast,
            Field::ReleaseDate => match fields.release_date.0 {
                Some(value) => value,
                None => empty(),
            },
            Field::EpisodeRunTime => fields.episode_run_time,
            Field::DetailedFetched => fields.detailed_fetched,
            Field::EpisodesFetched => fields.episodes_fetched,
            Field::DetailedInfo => fields.detailed_info,
            Field::BasicData => fields.basic_data,
        })
    }
    pub fn release_date_present(&self) -> Result<Option<&'a RawValue>, Error> {
        let Shape::Mapping(fields) = &self.0 else {
            return Err(Error::SourceObject);
        };
        Ok(fields.release_date.0)
    }
    /// prefer_relation_artwork deliberately tolerates a nonmapping object.
    pub fn artwork(&self, key: Field) -> Option<&'a RawValue> {
        self.get(key).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_null_and_falsey_values_are_distinct() {
        let p = Properties::parse(Some(
            r#"{"director":null,"cast":false,"actors":[],"release_date":0}"#,
        ))
        .unwrap();
        assert_eq!(p.get(Field::Director).unwrap(), "null");
        assert_eq!(p.get(Field::Cast).unwrap(), "false");
        assert_eq!(p.get(Field::Actors).unwrap(), "[]");
        assert_eq!(p.get(Field::ReleaseDate).unwrap(), "0");
        assert_eq!(p.get(Field::Trailer).unwrap(), "\"\"");
    }
    #[test]
    fn nonmapping_failure_is_deferred_to_source_get_boundary() {
        for raw in ["true", "1", "[1]", "\"truthy\""] {
            let p = Properties::parse(Some(raw)).unwrap();
            assert!(p.artwork(Field::MovieImage).is_none());
            assert!(matches!(p.get(Field::Trailer), Err(Error::SourceObject)));
        }
        for raw in ["null", "false", "0", "0.0", "[]", "\"\""] {
            assert_eq!(
                Properties::parse(Some(raw))
                    .unwrap()
                    .get(Field::Trailer)
                    .unwrap(),
                "\"\""
            );
        }
    }
    #[test]
    fn literal_private_keys_and_selected_subtrees_are_not_value_reinterpreted() {
        let raw = r#"{"$serde_json::private::RawValue":"[1]","director":{"$serde_json::private::Number":"12"},"detailed_fetched":true,"episodes_fetched":false,"detailed_info":{"x":1},"basic_data":[1],"episode_run_time":null,"movie_image":"x","backdrop_path":["y"]}"#;
        let p = Properties::parse(Some(raw)).unwrap();
        assert_eq!(
            p.get(Field::Director).unwrap(),
            r#"{"$serde_json::private::Number":"12"}"#
        );
        for field in [
            Field::MovieImage,
            Field::Backdrop,
            Field::DetailedFetched,
            Field::EpisodesFetched,
            Field::DetailedInfo,
            Field::BasicData,
            Field::EpisodeRunTime,
        ] {
            assert!(p.get(field).is_ok());
        }
    }
}
