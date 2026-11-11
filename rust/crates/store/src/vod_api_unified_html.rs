//! Isolated, bounded pinned-DRF BrowsableAPI renderer prototype.
//! Derived template whitespace and algorithms retain original Dispatcharr,
//! Django Software Foundation, Encode/DRF and Pygments author credit.
//! The four captured list contexts are supported; other HTML contexts remain
//! explicit unsupported until independently captured. No session auth invented.
use super::vod_api_owned_read::{Document,ReadError};
use std::io::Write;
const PLAIN:&str=include_str!("../datasets/unified-html-source-1/accept_HTML.html.in");
const STANDARD:&str=include_str!("../datasets/unified-html-source-1/format_api_HTML_standard.html.in");
const ADMIN:&str=include_str!("../datasets/unified-html-source-1/format_api_HTML_admin.html.in");
const ANONYMOUS:&str=include_str!("../datasets/unified-html-source-1/format_api_HTML_None.html.in");
const ALPHABET:&[u8]=b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
#[derive(Clone,Copy)]
pub(super) enum Kind { Plain,Standard,Admin,Anonymous }
impl Kind {
    fn template(self)->&'static str {match self{Self::Plain=>PLAIN,Self::Standard=>STANDARD,Self::Admin=>ADMIN,Self::Anonymous=>ANONYMOUS}}
}
fn write(out:&mut Document,text:&str)->Result<(),ReadError>{out.write_all(text.as_bytes()).map_err(|_|ReadError::Capacity)}
fn escape(out:&mut Document,text:&str)->Result<(),ReadError>{
    for c in text.chars(){match c{'&'=>write(out,"&amp;")?,'<'=>write(out,"&lt;")?,'>'=>write(out,"&gt;")?,'"'=>write(out,"&quot;")?,'\''=>write(out,"&#x27;")?,other=>{let mut bytes=[0;4];write(out,other.encode_utf8(&mut bytes))?;}}}Ok(())
}
fn random_token()->String {
    let mut result=String::with_capacity(32);
    while result.len()<32 {
        let random=uuid::Uuid::new_v4();
        // Exclude UUID version/variant bytes. Remaining bytes are independent
        // OS randomness; rejection sampling avoids modulo62 bias.
        for (index,byte) in random.as_bytes().iter().enumerate(){
            if index==6||index==8||*byte>=248{continue;}
            result.push(ALPHABET[usize::from(*byte)%62]as char);if result.len()==32{break;}
        }
    }result
}
fn indices(raw:&str)->Option<Vec<usize>> {raw.bytes().map(|c|ALPHABET.iter().position(|a|*a==c)).collect()}
pub(super) fn unmask(token:&str)->Option<String>{
    if token.len()!=64{return None;}let values=indices(token)?;
    Some((0..32).map(|i|ALPHABET[(values[i+32]+62-values[i])%62]as char).collect())
}
pub(super) struct Csrf {pub secret:String,pub masked:String}
impl Csrf {
    pub fn new(cookie:Option<&str>)->Self {
        let supplied=cookie.and_then(|raw|match raw.len(){32=>indices(raw).map(|_|raw.to_owned()),64=>unmask(raw),_=>None});
        let secret=supplied.unwrap_or_else(random_token);let mask=random_token();let a=indices(&secret).expect("validated CSRF secret");let b=indices(&mask).expect("OS-generated CSRF mask");
        let mut masked=mask;for i in 0..32 {masked.push(ALPHABET[(a[i]+b[i])%62]as char);}Self{secret,masked}
    }
    pub fn set_cookie(&self)->String {
        let expires=chrono::Utc::now()+chrono::Duration::seconds(31_449_600);
        format!("csrftoken={}; expires={}; Max-Age=31449600; Path=/; SameSite=Lax",self.secret,expires.format("%a, %d %b %Y %H:%M:%S GMT"))
    }
}
fn url_word(out:&mut Document,word:&str)->Result<(),ReadError>{
    // Captured Django6 URL families: ASCII HTTP(S) URLs, including query &,
    // path/query percent encoding and parentheses/brackets. Other linking
    // families remain explicit unaccepted, never called full urlize parity.
    let middle=word.trim_start_matches(['(','[']);let lead=&word[..word.len()-middle.len()];let mut end=middle.len();
    loop {
        let before=end;let part=&middle[..end];
        for (opening,closing)in [('(',')'),('[',']')]{
            let opens=part.chars().filter(|c|*c==opening).count();let closes=part.chars().filter(|c|*c==closing).count();
            if closes>opens {let excess=closes-opens;let trailing=middle[..end].chars().rev().take_while(|c|*c==closing).count();end-=excess.min(trailing);}
        }
        while end>0&&matches!(middle.as_bytes()[end-1],b'.'|b','|b':'|b';'|b'!'){end-=1;}
        if before==end{break;}
    }
    let url=&middle[..end];let scheme=url.get(..7).is_some_and(|v|v.eq_ignore_ascii_case("http://"))||url.get(..8).is_some_and(|v|v.eq_ignore_ascii_case("https://"));
    if (scheme&&!url.is_ascii())||url.starts_with("www.")||url.contains('@')||(!scheme&&[".com",".edu",".gov",".int",".mil",".net",".org"].iter().any(|suffix|url.ends_with(suffix)))||(scheme&&url.contains('\\')) {
        return Err(ReadError::Unsupported("Uncaptured Django URL linkification family"));
    }
    if scheme&&url.len()<=2048&&url.is_ascii() {
        escape(out,lead)?;write(out,"<a href=\"")?;escape(out,url)?;write(out,"\" rel=\"nofollow\">")?;escape(out,url)?;write(out,"</a>")?;escape(out,&middle[end..])
    }else{escape(out,word)}
}
fn content(out:&mut Document,pretty_json:&str)->Result<(),ReadError>{
    let mut word=0;for(index,c)in pretty_json.char_indices(){if c.is_whitespace()||matches!(c,'<'|'>'|'"'|'\''){
        if word<index{url_word(out,&pretty_json[word..index])?;}let mut bytes=[0;4];escape(out,c.encode_utf8(&mut bytes))?;word=index+c.len_utf8();
    }}if word<pretty_json.len(){url_word(out,&pretty_json[word..])?;}Ok(())
}
pub(super) fn render(kind:Kind,username:&str,method:&str,pretty_json:&str,csrf:&Csrf,mut out:Document)->Result<Vec<u8>,ReadError>{
    let source=kind.template();let mut remaining=source;
    while let Some(start)=remaining.find("@@DRF_"){
        write(&mut out,&remaining[..start])?;let token=&remaining[start..];let end=token[2..].find("@@").ok_or(ReadError::WorkerFailed)?+4;
        match &token[..end]{"@@DRF_USERNAME@@"=>escape(&mut out,username)?,"@@DRF_METHOD@@"=>escape(&mut out,method)?,"@@DRF_CSRF@@"=>write(&mut out,&csrf.masked)?,"@@DRF_CONTENT@@"=>content(&mut out,pretty_json)?,_=>return Err(ReadError::WorkerFailed)}
        remaining=&token[end..];
    }write(&mut out,remaining)?;Ok(out.finish())
}

#[derive(Clone)]
pub(super) struct ReplyHeaders {pub status:u16,pub set_cookie:String,pub authenticate:bool}
pub(super) fn requested(headers:&axum::http::HeaderMap)->bool {
    headers.get("accept").is_some_and(|value|value.as_bytes()==b"text/html")
}
pub(super) fn cookie(headers:&axum::http::HeaderMap)->Option<String>{
    let raw=headers.get("cookie").map(|v|dispatcharr_contracts::source_header_value(v.as_bytes()))?;
    raw.split(';').filter_map(|part|part.trim().split_once('=')).filter(|(key,_)|*key=="csrftoken").map(|(_,value)|value.trim().to_owned()).next_back()
}
pub(super) fn context(uri:&axum::http::Uri,identity:Option<(&str,i32)>,status:u16)->Result<(Kind,String),ReadError>{
    if uri.path()!="/api/vod/all/" {return Err(ReadError::Unsupported("Browsable detail HTML needs independent Source template proof"));}
    match (uri.query(),identity,status){
        (None,Some((username,level)),200)if(1..10).contains(&level)=>Ok((Kind::Plain,username.to_owned())),
        (Some("format=api"),Some((username,level)),200)if(1..10).contains(&level)=>Ok((Kind::Standard,username.to_owned())),
        (Some("format=api"),Some((username,level)),200)if level>=10=>Ok((Kind::Admin,username.to_owned())),
        (Some("format=api"),None,401)=>Ok((Kind::Anonymous,String::new())),
        _=>Err(ReadError::Unsupported("Browsable request/role/status context needs independent Source proof")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn four_actual_source_variants_and_head_lengths_preserve_every_deterministic_byte(){
        let source:serde_json::Value=serde_json::from_str(include_str!("../datasets/unified-html-source-1/renderer-source.json")).unwrap();
        for case in source["cases"].as_array().unwrap(){
            let kind=match case["kind"].as_str().unwrap(){"accept_HTML"=>Kind::Plain,"format_api_HTML_standard"=>Kind::Standard,"format_api_HTML_admin"=>Kind::Admin,"format_api_HTML_None"=>Kind::Anonymous,_=>unreachable!()};
            let csrf=Csrf::new(Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));assert_eq!(unmask(&csrf.masked).as_deref(),Some(csrf.secret.as_str()));
            let body=render(kind,case["username"].as_str().unwrap(),"GET",case["pretty_JSON_from_observed_HTML"].as_str().unwrap(),&csrf,Document::bounded(1024*1024)).unwrap();
            let expected=case["expected_HTML_with_only_declared_random_CSRF_slot"].as_str().unwrap().replace("@@INDEPENDENT_CSRF_MASK@@",&csrf.masked);
            assert_eq!(String::from_utf8(body.clone()).unwrap(),expected,"{} deterministic bytes",case["spec"]);assert_eq!(body.len()as u64,case["Source_body_bytes"].as_u64().unwrap());
            let head=render(kind,case["username"].as_str().unwrap(),"HEAD",case["pretty_JSON_from_observed_HTML"].as_str().unwrap(),&csrf,Document::bounded(1024*1024)).unwrap();assert_eq!(head.len()as u64,case["Source_HEAD_content_length"].as_u64().unwrap());
        }
    }
    #[test]
    fn csrf_fresh_mask_unmask_cookie_reuse_and_legacy64_are_consistent(){
        let first=Csrf::new(None);assert_eq!(first.secret.len(),32);assert_eq!(first.masked.len(),64);assert_eq!(unmask(&first.masked),Some(first.secret.clone()));
        let next=Csrf::new(Some(&first.secret));assert_eq!(next.secret,first.secret);assert_ne!(next.masked,first.masked);assert_eq!(unmask(&next.masked),Some(first.secret.clone()));
        let legacy=Csrf::new(Some(&first.masked));assert_eq!(legacy.secret,first.secret);assert_eq!(unmask(&legacy.masked),Some(first.secret.clone()));
        assert!(first.set_cookie().ends_with("; Max-Age=31449600; Path=/; SameSite=Lax"));
    }
    #[test]
    fn bounded_html_escaping_and_unknown_linkification_never_silently_truncate(){
        let csrf=Csrf::new(None);
        assert!(render(Kind::Plain,"name","GET","{}",&csrf,Document::bounded(128)).is_err());
        let body=render(Kind::Plain,"<script>'&\"","GET","{\"quote\":\"<&\"}",&csrf,Document::bounded(1024*1024)).unwrap();let body=String::from_utf8(body).unwrap();assert!(!body.contains("<script>'"));assert!(body.contains("&lt;script&gt;&#x27;&amp;&quot;"));
        assert!(matches!(render(Kind::Plain,"name","GET","\"www.example.com\"",&csrf,Document::bounded(1024*1024)),Err(ReadError::Unsupported(_))));
    }
}

#[cfg(test)]
#[path="vod_api_unified_html_peer.rs"]
mod peer;
#[cfg(test)]
pub(super) async fn compare_original_network(){peer::compare_original_network().await;}
