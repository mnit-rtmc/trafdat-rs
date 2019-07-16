// sensor.rs
//
// Copyright (c) 2019  Minnesota Department of Transportation
//
use actix_web::HttpResponse;
use std::fs::{File, read_dir};
use std::fmt::Display;
use std::fmt::Write;
use std::io::Read;
use std::path::{Path, PathBuf};
use zip::ZipArchive;

/// Base traffic archive path
const BASE_PATH: &str = "/var/lib/iris/traffic";

/// Default district ID
const DISTRICT_DEFAULT: &str = "tms";

/// Traffic file extension
const DEXT: &str = ".traffic";

/// Traffic file extension without dot
const EXT: &str = "traffic";

/// Extension fragments for sample types, plus sample bytes
const SAMPLE_TYPES: &[(&str, u64)] = &[
    ("vmc", 1), ("vs", 1), ("vm", 1), ("vl", 1),
    ("v", 1), ("o", 2), ("c", 2), ("s", 1),
    ("pr", 2), ("pt", 1),
];

/// Tuples of sample period, number of samples per day
const SAMPLE_PERIODS: &[(&str, u64)] = &[
    ("60", 1440), // <- deprecated binning interval (precipitation rate)
    ("30", 2880), ("20", 4320), ("15", 5760),
    ("10", 8640), ("6", 14400), ("5", 17280),
];

/// Build responses from data
trait ResponseBuilder {
    fn build(data: Option<Vec<u8>>) -> Option<HttpResponse>;
}

/// JSON response output
struct JsonOutput;

/// Build JSON response from data
impl ResponseBuilder for JsonOutput {
    fn build(data: Option<Vec<u8>>) -> Option<HttpResponse> {
        data.and_then(|b| json_response(build_json(b)))
    }
}

/// Build JSON response from a Vec
fn build_json<T: Display>(arr: Vec<T>) -> Option<String> {
    if arr.len() > 0 {
        let mut res = "[".to_string();
        for val in arr {
            if res.len() > 1 {
                res.push(',');
            }
            res.push('"');
            write!(&mut res, "{}", val).unwrap();
            res.push('"');
        }
        res.push(']');
        Some(res)
    } else {
        None
    }
}

/// Create a JSON response
fn json_response(json: Option<String>) -> Option<HttpResponse> {
    json.and_then(|j| Some(HttpResponse::Ok()
        .content_type("application/json")
        .body(j))
    )
}

/// Octet stream response output
struct OctetStreamOutput;

/// Build octet stream response from data
impl ResponseBuilder for OctetStreamOutput {
    fn build(data: Option<Vec<u8>>) -> Option<HttpResponse> {
        data.and_then(|b| Some(HttpResponse::Ok()
            .content_type("application/octet_stream")
            .body(b))
        )
    }
}

/// List files in a directory or zip file
trait FileLister {

    /// Check a file or zip entry by name
    fn check<'a, 'b>(&'a self, name: &'b str, dir: bool) -> Option<&'b str>;

    /// Get a list of entries in a directory
    fn list_dir(&self, path: &Path) -> Vec<String> {
        let mut list = vec![];
        if let Ok(entries) = read_dir(path) {
            for entry in entries {
                if let Ok(ent) = entry {
                    if let Ok(tp) = ent.file_type() {
                        if !tp.is_symlink() {
                            if let Some(name) = ent.file_name().to_str() {
                                if let Some(e) = self.check(name, tp.is_dir()) {
                                    list.push(e.to_string())
                                }
                            }
                        }
                    }
                }
            }
        }
        list
    }

    /// Get a list of entries in a zip file
    fn list_zip(&self, path: &Path) -> Vec<String> {
        let mut list = vec![];
        if let Ok(file) = File::open(path) {
            if let Ok(mut zip) = ZipArchive::new(file) {
                for i in 0..zip.len() {
                    if let Ok(zf) = zip.by_index(i) {
                        let ent = Path::new(zf.name());
                        if let Some(name) = ent.file_name() {
                            if let Some(name) = name.to_str() {
                                if let Some(e) = self.check(name, false) {
                                    list.push(e.to_string())
                                }
                            }
                        }
                    }
                }
            }
        }
        list
    }
}

/// Lister for directories
struct DirLister;

impl FileLister for DirLister {
    fn check<'a, 'b>(&'a self, name: &'b str, dir: bool) -> Option<&'b str> {
        match dir {
            true => Some(name),
            false => None,
        }
    }
}

/// Lister for valid dates
struct DateLister;

impl FileLister for DateLister {
    fn check<'a, 'b>(&'a self, name: &'b str, dir: bool) -> Option<&'b str> {
        if dir {
            if is_valid_date(name) {
                 return Some(name)
            }
        } else if name.len() == 16 && name.ends_with(DEXT) {
            let date = &name[..8];
            if is_valid_date(date) {
                 return Some(date)
            }
        }
        None
    }
}

/// Lister for sensor IDs
struct SidLister;

impl FileLister for SidLister {
    fn check<'a, 'b>(&'a self, name: &'b str, dir: bool) -> Option<&'b str> {
        if !dir {
            let path = Path::new(name);
            path.extension()
                .and_then(|ext| ext.to_str())
                .and_then(|ext| sample_file_ext(ext))
                .and_then(|_| path.file_stem())
                .and_then(|f| f.to_str())
        } else {
            None
        }
    }
}

/// Lister for sample file extensions
struct ExtLister<'s> {
    sid: &'s str,
}

impl<'s> FileLister for ExtLister<'s> {
    fn check<'a, 'b>(&'a self, name: &'b str, dir: bool) -> Option<&'b str> {
        if !dir {
            let path = Path::new(name);
            path.file_stem()
                .and_then(|st| if st == self.sid { Some(()) } else { None })
                .and_then(|_| path.extension())
                .and_then(|ext| ext.to_str())
                .and_then(|ext| sample_file_ext(ext))
        } else {
            None
        }
    }
}

/// Parse year parameter
fn parse_year(year: &str) -> Option<i32> {
    year.parse().ok().filter(|yr| *yr >= 1900 && *yr <= 9999)
}

/// Parse month parameter
fn parse_month(month: &str) -> Option<i32> {
    month.parse().ok().filter(|mo| *mo >= 1 && *mo <= 12)
}

/// Parse day parameter
fn parse_day(day: &str) -> Option<i32> {
    day.parse().ok().filter(|da| *da >= 1 && *da <= 31)
}

/// Check if a date is valid
fn is_valid_date(date: &str) -> bool {
    date.len() == 8 &&
    parse_year(&date[..4]).is_some() &&
    parse_month(&date[4..6]).is_some() &&
    parse_day(&date[6..8]).is_some()
}

/// Check if year and date are valid
fn is_valid_year_date(year: &str, date: &str) -> bool {
    parse_year(year).is_some() && is_valid_date(date)
}

/// Handle request for dates in a year
fn handle_dates_text(district: &str, year: &str) -> Option<String> {
    let mut dates = lookup_dates(district, year);
    if dates.len() > 0 {
        dates.sort();
        let mut res = String::new();
        for date in dates {
            res.push_str(&date);
            res.push('\n');
        }
        Some(res)
    } else {
        None
    }
}

/// Lookup all sampled dates in a year
fn lookup_dates(district: &str, year: &str) -> Vec<String> {
    let lister = DateLister {};
    let mut path = PathBuf::from(BASE_PATH);
    path.push(district);
    path.push(year);
    // FIXME: use streaming from a separate thread
    lister.list_dir(&path)
}

/// Handle request for /did/year (plain text)
fn handle_did_year(district: &str, year: &str) -> Option<HttpResponse> {
    parse_year(year)
        .and_then(|_| handle_dates_text(district, year))
        .and_then(|d| Some(HttpResponse::Ok()
            .content_type("text/plain").body(d)))
}

/// Handle request for /did/date (JSON)
fn handle_did_date(district: &str, date: &str) -> Option<HttpResponse> {
    if is_valid_date(date) {
        json_response(build_json(lookup_sensors(district, date)))
    } else {
        None
    }
}

/// Lookup sampled sensors for one date
fn lookup_sensors(district: &str, date: &str) -> Vec<String> {
    let mut path = PathBuf::from(BASE_PATH);
    path.push(district);
    path.push(&date[..4]);
    path.push(date);
    // FIXME: use streaming from a separate thread
    let lister = SidLister {};
    let mut sensors = lister.list_dir(&path);
    path.set_extension(EXT);
    sensors.extend(lister.list_zip(&path));
    sensors
}

/// Check a sample file extension
fn sample_file_ext(ext: &str) -> Option<&str> {
    if ext == "vlog" {
        return Some(ext)
    }
    if let Some((prefix, _)) = sample_type(ext) {
        if let Some((suffix, _)) = sample_period(ext) {
            if prefix.len() + suffix.len() == ext.len() {
                return Some(ext)
            }
        }
    }
    None
}

/// Get sample type prefix and length for an extension
fn sample_type(ext: &str) -> Option<(&str, u64)> {
    for (prefix, len) in SAMPLE_TYPES {
        if ext.starts_with(prefix) {
            return Some((prefix, *len))
        }
    }
    None
}

/// Get sample period suffix and length for an extension
fn sample_period(ext: &str) -> Option<(&str, u64)> {
    for (suffix, len) in SAMPLE_PERIODS {
        if ext.ends_with(suffix) {
            return Some((suffix, *len))
        }
    }
    None
}

/// Check length of a sample file with extension
fn is_valid_sample_len(ext: &str, len: u64) -> bool {
    if ext == "vlog" {
        return true
    }
    if let Some((prefix, tlen)) = sample_type(ext) {
        if let Some((suffix, plen)) = sample_period(ext) {
            if prefix.len() + suffix.len() == ext.len() {
                return (tlen * plen) == len
            }
        }
    }
    false
}

/// Handle request for sensors sampled on a date
fn handle_did_year_date(district: &str, year: &str, date: &str)
    -> Option<HttpResponse>
{
    if is_valid_year_date(year, date) {
        if &date[..4] == year {
            handle_did_date(district, date)
        } else {
            Some(bad_request())
        }
    } else {
        None
    }
}

/// Handle bad request
fn bad_request() -> HttpResponse {
    HttpResponse::BadRequest().body("Bad request")
}

/// Handle request for sampled dates /did/year (JSON)
fn handle_did_year_json(district: &str, year: &str) -> Option<HttpResponse> {
    parse_year(year).and_then(|_| lookup_dates_json(district, year))
}

/// Lookup all sampled dates in a year (JSON)
fn lookup_dates_json(district: &str, year: &str) -> Option<HttpResponse> {
    json_response(build_json(lookup_dates(district, year)))
}

/// Handle request for sampled data
fn handle_did_date_sidext<B>(district: &str, date: &str, sid_ext: &str)
    -> Option<HttpResponse>
    where B: ResponseBuilder
{
    let mut sp = sid_ext.splitn(2, '.');
    if let Some(sid) = sp.next() {
        if let Some(ext) = sp.next() {
            return handle_did_date_sid_ext::<B>(district, date, sid, ext)
        }
    }
    None
}

/// Handle request for sampled data
fn handle_did_date_sid_ext<B>(district: &str, date: &str, sid: &str, ext: &str)
    -> Option<HttpResponse>
    where B: ResponseBuilder
{
    if is_valid_date(date) && sample_file_ext(ext).is_some() {
        let mut path = PathBuf::from(BASE_PATH);
        path.push(district);
        path.push(&date[..4]);
        path.push(date);
        B::build(read_path_sid_ext(&mut path, sid, ext))
    } else {
        None
    }
}

/// Read sampled data from a path
fn read_path_sid_ext(path: &mut PathBuf, sid: &str, ext: &str)
    -> Option<Vec<u8>>
{
    path.push(sid);
    path.set_extension(ext);
    // FIXME: handle rebinning?
    if let Ok(mut file) = File::open(&path) {
        if let Ok(metadata) = file.metadata() {
            let len = metadata.len();
            if is_valid_sample_len(ext, len) {
                let mut data = vec![0; len as usize];
                if let Ok(()) = file.read_exact(&mut data[..]) {
                    return Some(data)
                }
            }
        }
    } else {
        path.pop(); // sid.ext
        path.set_extension(EXT);
        if let Ok(file) = File::open(path) {
            if let Ok(mut zip) = ZipArchive::new(file) {
                let name = format!("{}.{}", sid, ext);
                if let Ok(mut zf) = zip.by_name(&name) {
                    let len = zf.size();
                    if is_valid_sample_len(ext, len) {
                        let mut data = vec![0; len as usize];
                        if let Ok(()) = zf.read_exact(&mut data[..]) {
                            return Some(data)
                        }
                    }
                }
            }
        }
    }
    // FIXME: open .vlog
    None
}

/// Handle request for sampled extensions
fn handle_did_date_sid(district: &str, date: &str, sid: &str)
    -> Option<HttpResponse>
{
    if is_valid_date(date) {
        json_response(build_json(lookup_ext(district, date, sid)))
    } else {
        None
    }
}

/// Lookup sampled extensions for a sensor
fn lookup_ext(district: &str, date: &str, sid: &str) -> Vec<String> {
    let mut path = PathBuf::from(BASE_PATH);
    path.push(district);
    path.push(&date[..4]); // year
    path.push(date);
    let lister = ExtLister { sid };
    let mut exts = lister.list_dir(&path);
    path.set_extension(EXT);
    exts.extend(lister.list_zip(&path));
    exts
}

/// Handle request for sampled data
fn handle_did_year_date_sidext<B>(district: &str, year: &str, date: &str,
    sid_ext: &str) -> Option<HttpResponse>
    where B: ResponseBuilder
{
    if is_valid_year_date(year, date) {
        if &date[..4] == year {
            handle_did_date_sidext::<B>(district, date, sid_ext)
        } else {
            Some(bad_request())
        }
    } else {
        None
    }
}

/// Handle districts request
pub fn handle_districts_json() -> Option<HttpResponse> {
    let lister = DirLister {};
    let path = PathBuf::from(BASE_PATH);
    json_response(build_json(lister.list_dir(&path)))
}

/// Handle request with one parameter
pub fn handle_1_param(year: &str) -> Option<HttpResponse> {
    handle_did_year(DISTRICT_DEFAULT, year)
}

/// Handle JSON request with two parameters
pub fn handle_2_params_json(p1: &str, p2: &str) -> Option<HttpResponse> {
    handle_did_year_json(p1, p2)
}

/// Handle request with two parameters
pub fn handle_2_params(p1: &str, p2: &str) -> Option<HttpResponse> {
    handle_did_date(p1, p2)
        .or_else(|| handle_did_year_date(DISTRICT_DEFAULT, p1, p2))
        .or_else(|| handle_did_year(p1, p2))
}

/// Handle JSON request with three parameters
pub fn handle_3_params_json(p1: &str, p2: &str, p3: &str)
    -> Option<HttpResponse>
{
    handle_did_date_sidext::<JsonOutput>(p1, p2, p3)
        .or_else(|| handle_did_date_sid(p1, p2, p3))
        .or_else(|| handle_did_year_date_sidext::<JsonOutput>(DISTRICT_DEFAULT,
            p1, p2, p3))
}

/// Handle request with three parameters
pub fn handle_3_params(p1: &str, p2: &str, p3: &str) -> Option<HttpResponse> {
    handle_did_date_sidext::<OctetStreamOutput>(p1, p2, p3)
        .or_else(|| handle_did_year_date_sidext::<OctetStreamOutput>(
            DISTRICT_DEFAULT, p1, p2, p3))
        .or_else(|| handle_did_year_date(p1, p2, p3))
}
