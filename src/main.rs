// main.rs
//
// Copyright (c) 2019-2021  Minnesota Department of Transportation
//
#![forbid(unsafe_code)]

mod error;
mod metro;
mod sensor;

use crate::error::Error;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use log::error;

/// Index page
const INDEX_HTML: &str = include_str!("index.html");

/// CSS for index page
const TRAFDAT_CSS: &str = include_str!("trafdat.css");

/// Main function
fn main() {
    env_logger::builder().format_timestamp(None).init();
    let res = run_server("0.0.0.0:8080");
    if let Err(e) = &res {
        error!("{:?}", e);
        res.unwrap();
    }
}

/// Run web server at given socket
fn run_server(sock_addr: &str) -> Result<(), Error> {
    HttpServer::new(|| {
        App::new()
            .service(
                web::scope("/trafdat")
                    .route("/", web::to(|| handle_index()))
                    .route("/index.html", web::to(|| handle_index()))
                    .route("/trafdat.css", web::to(|| handle_css()))
                    .route("/districts", web::to(|| handle_districts()))
                    .route("/{p1}", web::to(handle_1))
                    .route(
                        "/metro_config/{p1}.json",
                        web::to(handle_metro_1_json),
                    )
                    .route(
                        "/metro_config/{p1}.xml",
                        web::to(handle_metro_1_xml),
                    )
                    .route("/{p1}/{p2}.json", web::to(handle_2_json))
                    .route("/{p1}/{p2}", web::to(handle_2))
                    .route(
                        "/metro_config/{p1}/corridors",
                        web::to(handle_metro_corridors),
                    )
                    .route(
                        "/metro_config/{p1}/{p2}_{p3}.json",
                        web::to(handle_metro_3_json),
                    )
                    .route(
                        "/metro_config/{p1}/{p2}_{p3}.xml",
                        web::to(handle_metro_3_xml),
                    )
                    .route("/{p1}/{p2}/{p3}.json", web::to(handle_3_json))
                    .route("/{p1}/{p2}/{p3}", web::to(handle_3)),
            )
            .default_service(web::route().to(|| not_found()))
    })
    .bind(sock_addr)?
    .run()?;
    Ok(())
}

/// Handle a request for index page
fn handle_index() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/html")
        .body(INDEX_HTML)
}

/// Handle a request for CSS
fn handle_css() -> HttpResponse {
    HttpResponse::Ok()
        .content_type("text/css")
        .body(TRAFDAT_CSS)
}

/// Handle a request for districts
fn handle_districts() -> HttpResponse {
    sensor::handle_districts_json().unwrap_or_else(|| not_found())
}

/// Handle not found requests
fn not_found() -> HttpResponse {
    HttpResponse::NotFound().body("Not Found")
}

/// Handle a request with one parameter
fn handle_metro_1_xml(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| metro::handle_1_param_xml(p1))
        .unwrap_or_else(|| not_found())
}

/// Handle a request with one parameter
fn handle_metro_1_json(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| metro::handle_1_param_json(p1))
        .unwrap_or_else(|| not_found())
}

/// Handle a request for the corridors on a date
fn handle_metro_corridors(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| metro::handle_corridors(p1))
        .unwrap_or_else(|| not_found())
}

/// Handle a request for metro_config xml with 2 parameters
fn handle_metro_3_xml(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info().get("p2").and_then(|p2| {
                req.match_info()
                    .get("p3")
                    .and_then(|p3| metro::handle_3_params_xml(p1, p2, p3))
            })
        })
        .unwrap_or_else(|| not_found())
}

/// Handle a request for metro_config json with 2 parameters
fn handle_metro_3_json(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info().get("p2").and_then(|p2| {
                req.match_info()
                    .get("p3")
                    .and_then(|p3| metro::handle_3_params_json(p1, p2, p3))
            })
        })
        .unwrap_or_else(|| not_found())
}

/// Handle a request with one parameter
fn handle_1(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| sensor::handle_1_param(p1))
        .unwrap_or_else(|| not_found())
}

/// Handle a JSON request with two parameters
fn handle_2_json(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info()
                .get("p2")
                .and_then(|p2| sensor::handle_2_params_json(p1, p2))
        })
        .unwrap_or_else(|| not_found())
}

/// Handle a request with two parameters
fn handle_2(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info()
                .get("p2")
                .and_then(|p2| sensor::handle_2_params(p1, p2))
        })
        .unwrap_or_else(|| not_found())
}

/// Handle a JSON request with three parameters
fn handle_3_json(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info().get("p2").and_then(|p2| {
                req.match_info()
                    .get("p3")
                    .and_then(|p3| sensor::handle_3_params_json(p1, p2, p3))
            })
        })
        .unwrap_or_else(|| not_found())
}

/// Handle a request with three parameters
fn handle_3(req: HttpRequest) -> HttpResponse {
    req.match_info()
        .get("p1")
        .and_then(|p1| {
            req.match_info().get("p2").and_then(|p2| {
                req.match_info()
                    .get("p3")
                    .and_then(|p3| sensor::handle_3_params(p1, p2, p3))
            })
        })
        .unwrap_or_else(|| not_found())
}
