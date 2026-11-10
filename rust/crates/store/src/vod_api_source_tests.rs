//! Test-only original-corpus DTO/policy assertions; no SQL/HTTP claims here.
//! These are projection tests using Source response-derived rows, NOT SQL/
//! HTTP/resource/provider acceptance. Actual native DB parity needs a separate
//! seeded PostgreSQL/Source gate. Original Dispatcharr authors retain credit.
use super::{vod_api_dto::*, vod_api_pagination, vod_api_projection};
use chrono::{DateTime, Utc};
use serde_json::Value;
fn source() -> Value {
    serde_json::from_str(include_str!("../datasets/vod-rest-source-1.json")).unwrap()
}
fn expected<'a>(source: &'a Value, path: &str, role: &str) -> &'a Value {
    let mode = &source["modes"]["http"];
    let case = mode["cases"].as_array().unwrap().iter().find(|case|
        case["path"] == path && case["role"] == role && case["method"] == "GET").unwrap();
    &mode["responses"][case["expected_ref"].as_str().unwrap()]
}
fn text(row: &Value, key: &str) -> String { row[key].as_str().unwrap().into() }
fn nullable(row: &Value, key: &str) -> Option<String> { row[key].as_str().map(str::to_owned) }
fn count(row: &Value, key: &str) -> i64 { row[key].as_i64().unwrap() }
fn timestamp(row: &Value, key: &str) -> DateTime<Utc> { DateTime::parse_from_rfc3339(row[key].as_str().unwrap()).unwrap().with_timezone(&Utc) }
fn common(row: &Value) -> CommonRow {
    CommonRow { id:count(row,"id"), uuid:text(row,"uuid"), name:text(row,"name"),
        description:nullable(row,"description"), year:row["year"].as_i64(),
        rating:if row["rating"].is_number(){Some(row["rating"].to_string())}else{nullable(row,"rating")},
        genre:nullable(row,"genre"), tmdb_id:nullable(row,"tmdb_id"), imdb_id:nullable(row,"imdb_id"),
        language:row["language"].as_str().unwrap_or("").into(), custom_properties:row["custom_properties"].clone(),
        created_at:timestamp(row,"created_at"), updated_at:timestamp(row,"updated_at") }
}
fn logo(value: &Value) -> Option<LogoRow> {
    if value.is_null(){return None;}
    let names = value["item_names"].as_array().cloned().unwrap_or_default();
    Some(LogoRow { id:count(value,"id"),name:text(value,"name"),url:text(value,"url"),
        movie_count:count(value,"movie_count"),series_count:count(value,"series_count"),is_used:value["is_used"].as_bool().unwrap(),
        movie_names:names.iter().filter_map(|v|v.as_str()?.strip_prefix("Movie: ").map(str::to_owned)).collect(),
        series_names:names.iter().filter_map(|v|v.as_str()?.strip_prefix("Series: ").map(str::to_owned)).collect() })
}
fn series(value: &Value) -> SeriesRow { SeriesRow { common:common(value),logo:logo(&value["logo"]),episode_count:count(value,"episode_count") } }
fn exact_value(actual: Value, expected: &Value) {
    // Comparing serialized bytes also checks insertion order/nullability/0.0.
    assert_eq!(serde_json::to_vec(&actual).unwrap(),serde_json::to_vec(expected).unwrap());
}
#[test]
fn original_dto_field_order_nulls_and_nested_context() {
    let source=source(); let context=LogoContext {absolute_origin:Some("http://localhost")};
    for endpoint in ["movies","series","episodes","all"] {
        let response=expected(&source,&format!("/api/vod/{endpoint}/"),"standard");
        assert_eq!(response["status"],200);
        for row in response["json"]["results"].as_array().unwrap() {
            let projected=match endpoint {
                "movies"=>MovieRow {common:common(row),logo:logo(&row["logo"]),duration_secs:row["duration_secs"].as_i64(),is_adult:row["is_adult"].as_bool().unwrap()}.project(&context),
                "series"=>series(row).project(&context),
                "episodes"=>EpisodeRow {id:count(row,"id"),uuid:text(row,"uuid"),name:text(row,"name"),series:series(&row["series"]),
                    description:nullable(row,"description"),air_date:row["air_date"].as_str().map(|v|chrono::NaiveDate::parse_from_str(v,"%Y-%m-%d").unwrap()),
                    rating:nullable(row,"rating"),duration_secs:row["duration_secs"].as_i64(),season_number:row["season_number"].as_i64(),
                    episode_number:row["episode_number"].as_i64(),tmdb_id:nullable(row,"tmdb_id"),imdb_id:nullable(row,"imdb_id"),
                    custom_properties:row["custom_properties"].clone(),created_at:timestamp(row,"created_at"),updated_at:timestamp(row,"updated_at")}.project(&context),
                "all"=>unified(&common(row),logo(&row["logo"]).as_ref(),row["duration"].as_i64(),row["content_type"].as_str().unwrap(),&context).unwrap(),
                _=>unreachable!(),
            };
            exact_value(projected,row);
        }
    }
}
#[test]
fn original_pagination_and_literal_false_visibility() {
    let source=source();
    for (raw, size, page, status) in [("page=last",20,2,200),("page=0",20,0,404),
        ("page_size=0",20,1,200),("page_size=-1",20,1,200),("page_size=bad",20,1,200),
        ("page_size=101",100,1,200),("page=2&page=1",20,1,200)] {
        let pairs:Vec<_>=url::form_urlencoded::parse(raw.as_bytes()).into_owned().collect();
        let result=vod_api_pagination::ordinary(&pairs,26);
        assert_eq!(expected(&source,&format!("/api/vod/movies/?{raw}"),"standard")["status"],status);
        if status==200 {let result=result.unwrap();assert_eq!((result.size,result.number),(size,page));}
        else {assert_eq!(result,Err(vod_api_pagination::InvalidPage));}
    }
    for (role,properties) in [("zero_flags",serde_json::json!({"vod_movies_enabled":0,"vod_series_enabled":0})),
        ("both_off",serde_json::json!({"vod_movies_enabled":false,"vod_series_enabled":false}))] {
        let visibility=vod_api_projection::visibility(&properties,1).unwrap();
        assert_eq!(visibility.movies,role=="zero_flags");
        assert_eq!(visibility.series,role=="zero_flags");
        let count=expected(&source,"/api/vod/movies/",role)["json"]["count"].as_u64().unwrap();
        assert_eq!(count,if visibility.movies {26}else{0});
    }
    // No source-refresh/native SQL correctness is inferred from these rows.
}

#[test]
fn original_category_projection_and_relative_nested_logo() {
    let source=source();
    let categories=expected(&source,"/api/vod/categories/","standard");
    for row in categories["json"].as_array().unwrap() {
        let accounts=row["m3u_accounts"].as_array().unwrap().iter().map(|account|CategoryAccountRow {
            category:count(account,"category"),m3u_account:count(account,"m3u_account"),
            enabled:account["enabled"].as_bool().unwrap(),custom_properties:account["custom_properties"].clone(),
        }).collect();
        exact_value(CategoryRow {id:count(row,"id"),name:text(row,"name"),category_type:text(row,"category_type"),accounts}.project(),row);
    }
    let movie=source["modes"]["http"]["fixtures"]["movie_ids"][0].as_i64().unwrap();
    let providers=expected(&source,&format!("/api/vod/movies/{movie}/providers/"),"basic");
    assert_eq!(providers["status"],200);
    let context=LogoContext {absolute_origin:None};
    for provider in providers["json"].as_array().unwrap() {
        let row=&provider["movie"];
        exact_value(MovieRow {common:common(row),logo:logo(&row["logo"]),duration_secs:row["duration_secs"].as_i64(),is_adult:row["is_adult"].as_bool().unwrap()}.project(&context),row);
    }
}
#[test]
fn original_page_link_query_rewrite() {
    let source=source();
    let mode=&source["modes"]["http"];
    for case in mode["cases"].as_array().unwrap() {
        let path=case["path"].as_str().unwrap();
        if case["role"]!="standard"||case["method"]!="GET"||!path.starts_with("/api/vod/movies/") {continue;}
        let (route,raw)=path.split_once('?').unwrap_or((path,""));
        if route!="/api/vod/movies/"{continue;}
        let response=&mode["responses"][case["expected_ref"].as_str().unwrap()];
        if response["status"]!=200{continue;}
        let json=&response["json"];
        let query:Vec<_>=url::form_urlencoded::parse(raw.as_bytes()).into_owned().collect();
        let page=vod_api_pagination::ordinary(&query,json["count"].as_u64().unwrap()).unwrap();
        let next=page.has_next().then(||vod_api_pagination::page_url("http://localhost",route,&query,Some(page.number+1)));
        let previous=page.has_previous().then(||vod_api_pagination::page_url("http://localhost",route,&query,
            (page.number>2).then_some(page.number-1)));
        assert_eq!(next.as_deref(),json["next"].as_str(),"{path}");
        assert_eq!(previous.as_deref(),json["previous"].as_str(),"{path}");
    }
}

#[test]
fn original_ordinary_raw_filter_subset_and_action_demands() {
    use super::vod_api_query::{self,Dispatch,Surface,QueryError};
    use super::vod_api_sql::Resource;
    use super::vod_api_projection::Permission;
    let source=source();let mode=&source["modes"]["http"];
    let mut seen=0;
    for case in mode["cases"].as_array().unwrap() {
        if case["role"]!="standard"||case["method"]!="GET"{continue;}
        let path=case["path"].as_str().unwrap();let (route,query)=path.split_once('?').unwrap_or((path,""));
        let resource=match route {"/api/vod/movies/"=>Resource::Movie,"/api/vod/series/"=>Resource::Series,
            "/api/vod/episodes/"=>Resource::Episode,_=>continue};
        let query:Vec<_>=url::form_urlencoded::parse(query.as_bytes()).into_owned().collect();
        let expected=&mode["responses"][case["expected_ref"].as_str().unwrap()];
        match vod_api_query::ordinary(resource,None,&query) {
            Err(QueryError::Source400(body))=>{assert_eq!(expected["status"],400);exact_value(body,&expected["json"]);},
            Ok(_)=>assert_ne!(expected["status"],400,"{path}"),
            Err(error)=>panic!("Captured ordinary query unexpectedly unsupported {path}: {error:?}"),
        }
        seen+=1;
    }
    assert!(seen>100);
    assert_eq!(vod_api_query::method(Surface::OrdinaryList,"HEAD").permission,Permission::Standard);
    assert_eq!(vod_api_query::method(Surface::OrdinaryList,"HEAD").dispatch,Dispatch::Get{head:true});
    assert_eq!(vod_api_query::method(Surface::Providers,"GET").permission,Permission::Authenticated);
    assert_eq!(vod_api_query::method(Surface::Image,"GET").permission,Permission::AllowAny);
    assert_eq!(vod_api_query::method(Surface::OrdinaryList,"OPTIONS").permission,Permission::Admin);
    assert_eq!(vod_api_query::method(Surface::OrdinaryList,"POST").dispatch,Dispatch::MethodNotAllowed);
}
