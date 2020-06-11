// metro.rs
//
// Copyright (c) 2020 Minnesota Department of Transportation
//
use actix_web::HttpResponse;
use std::io::Read;
use std::fs::File;
use std::path::{PathBuf};
use serde_xml_rs::{from_str};
use serde::{Serialize, Deserialize};
use serde_json;
use flate2::read::GzDecoder;
use libxml::parser::Parser;
use libxml::xpath::Context;
use libxml::tree::document::Document;
use unicode_segmentation::UnicodeSegmentation;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct TmsConfig {
    #[serde(default = "Vec::new")]
    corridor: Vec<Corridor>,
    #[serde(default = "Vec::new")]
    camera: Vec<Camera>,
    #[serde(default = "Vec::new")]
    commlink: Vec<Commlink>,
    #[serde(default = "Vec::new")]
    controller: Vec<Controller>,
    #[serde(default = "Vec::new")]
    dms: Vec<Dms>,
    time_stamp: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Corridor {
    #[serde(default = "Vec::new")]
    r_node: Vec<RNode>,
    route: String,
    dir: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct RNode {
    #[serde(default = "Vec::new")]
    detector: Vec<Detector>,
    #[serde(default = "Vec::new")]
    meter: Vec<Meter>,
    name: String,
    #[serde(default = "station_str")]
    n_type: String,
    #[serde(default = "false_str")]
    pickable: String,
    #[serde(default = "false_str")]
    above: String,
    #[serde(default = "none_str")]
    transition: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    station_id: String,
    #[serde(default = "String::new")]
    label: String,
    lon: String,
    lat: String,
    #[serde(default = "zero_str")]
    lanes: String,
    #[serde(default = "right_str")]
    attach_side: String,
    #[serde(default = "zero_str")]
    shift: String,
    #[serde(default = "true_str")]
    active: String,
    #[serde(default = "false_str")]
    abandoned: String,
    #[serde(default = "ff_str")]
    s_limit: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    forks: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Detector {
    name: String,
    #[serde(default = "future_str")]
    label: String,
    #[serde(default = "false_str")]
    abandoned: String,
    #[serde(default = "String::new")]
    category: String,
    #[serde(default = "zero_str")]
    lane: String,
    #[serde(default = "tt_str")]
    field: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    controller: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Meter {
    name: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lon: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lat: String,
    storage: String,
    #[serde(default = "tfz_str")]
    max_wait: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Camera {
    name: String,
    description: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lon: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lat: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Commlink {
    name: String,
    description: String,
    protocol: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Controller {
    name: String,
    //active: String,  // Present in XML DTD but not the actual document
    condition: String,  // Present in document, but not the DTD
    drop: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    commlink: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lon: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lat: String,
    location: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    cabinet: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    notes: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Dms {
    name: String,
    description: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lon: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    lat: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    width_pixels: String,
    #[serde(default = "implied_str")]
    #[serde(skip_serializing_if = "implied")]
    height_pixels: String,
}

/// Functions to implement defaults from the Document Type Definition (DTD)
fn station_str() -> String { "Station".to_string() }
fn false_str() -> String { "f".to_string() }
fn true_str() -> String { "t".to_string() }
fn zero_str() -> String { "0".to_string() }
fn none_str() -> String { "None".to_string() }
fn right_str() -> String { "right".to_string() }
fn ff_str() -> String { "55".to_string() }
fn tt_str() -> String { "22.0".to_string() }
fn tfz_str() -> String { "240".to_string() }
fn future_str() -> String { "FUTURE".to_string() }
/// Used as default for #IMPLIED attributes with no default
fn implied_str() -> String { "#IMPLIED".to_string() }
/// Used to check if #IMPLIED value should be left out
fn implied(val : &String) -> bool { val == "#IMPLIED" }

/// Base metro archive path
const BASE_PATH: &str = "/var/lib/iris/metro_config";

/// Takes the entire metro_config.xml string and converts it to
/// JSON using the above structs
fn build_full_json(xmldoc: Option<String>) -> Option<String> {
    xmldoc.and_then(|xmldoc| {
        let res : Result<TmsConfig, _> = from_str(&xmldoc);
        if let Ok(_tmsconfig) = res {
            if let Ok(js) = serde_json::to_string(&_tmsconfig) {
                Some(js)
            } else {
                None
            }
        } else {
            None
        }
    })
}

/// Takes a corridor's XML string and converts it to
/// JSON using the above structs
fn build_json(xmldoc: Option<String>) -> Option<String> {
    xmldoc.and_then(|xmldoc| {
        let res : Result<Corridor, _> = from_str(&xmldoc);
        if let Ok(_corridor) = res {
            if let Ok(js) = serde_json::to_string(&_corridor) {
                Some(js)
            } else {
                None
            }
        } else {
            None
        }
    })
}

/// Takes the XML string and builds the response
fn xml_response(xml: Option<String>) -> Option<HttpResponse> {
    xml.and_then(|x| Some(HttpResponse::Ok()
        .content_type("application/xml")
        .body(x))
    )
}

/// Takes the JSON string and builds the response
fn json_response(json: Option<String>) -> Option<HttpResponse> {
    json.and_then(|j| Some(HttpResponse::Ok()
        .content_type("application/json")
        .body(j))
    )
}

fn parse_year(year: &str) -> Option<i32> {
    year.parse().ok().filter(|yr| *yr >= 1900 && *yr <= 9999)
}

fn parse_month(month: &str) -> Option<i32> {
    month.parse().ok().filter(|mo| *mo >= 1 && *mo <= 12)
}

fn parse_day(day: &str) -> Option<i32> {
    day.parse().ok().filter(|da| *da >= 1 && *da <= 31)
}

fn is_valid_date(date: &str) -> bool {
    date.len() == 8 &&
    parse_year(&date[..4]).is_some() &&
    parse_month(&date[4..6]).is_some() &&
    parse_day(&date[6..8]).is_some()
}

/// Get the metro_config.xml.gz file for the specified date and extract it
fn get_xml_file(date: &str) -> Option<String> {
    let mut path = PathBuf::from(BASE_PATH);
    path.push(format!("metro_config_{}.xml.gz", date));
    if let Ok(file) = File::open(path) {
        let mut dec = GzDecoder::new(file);
        let mut metro_file = String::new();
        if let Ok(_) = dec.read_to_string(&mut metro_file) {
            return Some(metro_file);
        }
    }
    None
}

/// Using the metro config raw XML, find the proper corridor
fn get_corridor_on_date(metro_file_option: Option<String>, rte: &str, dir: &str) -> Option<String> {
    if let Some(metro_file) = metro_file_option {
        let parser : Parser = Default::default();
        let doc : Document = parser.parse_string(metro_file).unwrap();
        let mut context = Context::new(&doc).unwrap();
        let xpth : &str = &format!("//corridor[@route='{}' and @dir='{}']", rte, dir);
        if let Ok(cors) = context.findnodes(xpth, None) {
            if cors.len() > 0 {
                let cor = doc.node_to_string(&cors[0]);
                if cor.graphemes(true).count() > 0 {
                    return Some(cor);
                }
            }
        }
    }
    None
}

/// Handle metro_config XML request with one parameter (date)
pub fn handle_1_param_xml(p1: &str) -> Option<HttpResponse> {
    if is_valid_date(p1) {
        xml_response(get_xml_file(p1))
    } else {
        None
    }
}

/// Handle metro_config JSON request with one parameter (date)
pub fn handle_1_param_json(p1: &str) -> Option<HttpResponse> {
    if is_valid_date(p1) {
        json_response(build_full_json(get_xml_file(p1)))
    } else {
        None
    }
}

/// Handle metro_config XML request with two parameters (date, corridor, and direction)
pub fn handle_3_params_xml(p1: &str, p2: &str, p3: &str) -> Option<HttpResponse> {
    if is_valid_date(p1) {
        xml_response(get_corridor_on_date(get_xml_file(p1), p2, p3))
    } else {
        None
    }
}

/// Handle metro_config JSON request with two parameters (date, corridor, and direction)
pub fn handle_3_params_json(p1: &str, p2: &str, p3: &str) -> Option<HttpResponse> {
    if is_valid_date(p1) {
        json_response(build_json(get_corridor_on_date(get_xml_file(p1), p2, p3)))
    } else {
        None
    }
}
