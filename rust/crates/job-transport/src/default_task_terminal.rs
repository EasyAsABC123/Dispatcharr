//! Typed default-task sum; existing notification failure authority is unchanged.
//! Original Dispatcharr/Django/Celery authors retain behavior/code credit.
use crate::{default_protocol::Normal,notification_terminal,transport::Error};
use serde_json::json;
use uuid::Uuid;
#[derive(Debug)]
pub enum Terminal{Normal(Normal),Notification(notification_terminal::ProgrammingFailure),Scanner(ScannerFailure)}
impl From<notification_terminal::Terminal> for Terminal{
 fn from(value:notification_terminal::Terminal)->Self{let raised=value.raised();let mapped=match value{notification_terminal::Terminal::Normal(v)=>Self::Normal(v),notification_terminal::Terminal::Raised(v)=>Self::Notification(v)};debug_assert_eq!(raised,mapped.raised());mapped}
}
impl Terminal{pub(crate)fn raised(&self)->bool{!matches!(self,Self::Normal(_))}}
/// Own observed scanner failure. No generic Python exception construction,
/// arbitrary float grammar, SQL exception widening or notification reuse.
#[derive(Debug)]
pub struct ScannerFailure{_private:()}
impl ScannerFailure{
 pub(crate)fn observed_invalid_marker()->Self{Self{_private:()}}
 pub fn argument(&self)->&'static str{"could not convert string to float: 'owned-invalid-float'"}
 pub(crate)fn metadata(&self,id:Uuid,date:&str)->Result<Vec<u8>,Error>{
  if date.len()>40||chrono::DateTime::parse_from_rfc3339(date).is_err(){return Err(Error::Protocol)}
  let diagnostic="Native Dispatcharr logo scanner diagnostic [marker float conversion]: observed invalid marker; preceding physical reads settled. Original Python frames are not reproduced.";
  let raw=serde_json::to_vec(&json!({"status":"FAILURE","result":{"exc_type":"ValueError","exc_module":"builtins","exc_message":[self.argument()]},"traceback":diagnostic,"children":[],"date_done":date,"task_id":id.to_string()})).map_err(|_|Error::Protocol)?;
  crate::decode::decode(&raw,id).map_err(|_|Error::Protocol)?;Ok(raw)
 }
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]fn scanner_failure_is_own_bounded_valueerror_with_honest_diagnostic(){let id=Uuid::new_v4();let raw=ScannerFailure::observed_invalid_marker().metadata(id,"2026-11-11T12:00:00.000000+00:00").unwrap();let value:serde_json::Value=serde_json::from_slice(&raw).unwrap();assert_eq!(value["result"],json!({"exc_type":"ValueError","exc_module":"builtins","exc_message":["could not convert string to float: 'owned-invalid-float'"]}));assert!(value["traceback"].as_str().unwrap().starts_with("Native Dispatcharr"));assert!(!value["traceback"].as_str().unwrap().contains("Traceback (most recent call last)"));assert!(ScannerFailure::observed_invalid_marker().metadata(id,"invalid").is_err());}
}
