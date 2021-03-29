use std::convert::Infallible;

use bcrypt::verify;
use serde::de::DeserializeOwned;
use warp::{
    http::StatusCode,
    reject::{MissingHeader, Reject},
    reply::{Json, WithStatus},
    Filter,
    Rejection,
    Reply,
};

use crate::server::HASHED_SERVER_PASSWORD;

pub fn json_body<T: DeserializeOwned + Send>(
) -> impl Filter<Extract = (T,), Error = Rejection> + Copy {
    warp::body::content_length_limit(1024 * 1024).and(warp::body::json())
}

pub fn with_state<T: Clone + Send>(
    state: T,
) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || state.clone())
}

pub fn require_auth() -> impl Filter<Extract = ((),), Error = Rejection> + Copy {
    warp::header("X-RAV1E-AUTH").and_then(move |password: String| async move {
        if verify(password, &HASHED_SERVER_PASSWORD).unwrap() {
            return Ok(());
        }
        Err(warp::reject::custom(InvalidAuthorization))
    })
}

#[derive(Debug, Clone, Copy)]
pub struct InvalidAuthorization;

impl Reject for InvalidAuthorization {}

#[derive(Debug, Clone, Copy)]
pub struct ClientVersionMismatch;

impl Reject for ClientVersionMismatch {}

pub async fn handle_rejection_types(err: Rejection) -> Result<impl Reply, Rejection> {
    if err.find::<InvalidAuthorization>().is_some() {
        return Ok(warp::reply::with_status(
            "Incorrect server password".to_string(),
            StatusCode::UNAUTHORIZED,
        ));
    }
    if let Some(err) = err.find::<MissingHeader>() {
        if err.name() == "X-RAV1E-AUTH" {
            return Ok(warp::reply::with_status(
                "Password header not provided".to_string(),
                StatusCode::UNAUTHORIZED,
            ));
        }
    }
    if err.find::<ClientVersionMismatch>().is_some() {
        return Ok(warp::reply::with_status(
            "Client/server version mismatch".to_string(),
            StatusCode::BAD_REQUEST,
        ));
    }

    Err(err)
}

pub fn map_error_to_500<E: ToString>(_e: E) -> WithStatus<Json> {
    warp::reply::with_status(warp::reply::json(&()), StatusCode::INTERNAL_SERVER_ERROR)
}

#[macro_export]
macro_rules! try_or_500 {
    ( $expr:expr ) => {
        match $expr {
            Ok(val) => val,
            Err(e) => {
                return Ok(crate::server::helpers::map_error_to_500(e));
            }
        }
    };
}
