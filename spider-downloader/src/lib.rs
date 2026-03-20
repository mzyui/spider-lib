//! # spider-downloader
//!
//! Downloader traits and the default reqwest-based implementation used by the
//! crawler runtime.
//!
//! Depend on this crate directly when you want transport-level control without
//! reworking the request and response types used by the rest of the workspace.
//!
//! ## Example
//!
//! ```rust,ignore
//! use spider_downloader::Downloader;
//! use spider_util::{request::Request, response::Response, error::SpiderError};
//! use async_trait::async_trait;
//!
//! struct MyDownloader {
//!     client: reqwest::Client,
//! }
//!
//! #[async_trait]
//! impl Downloader for MyDownloader {
//!     type Client = reqwest::Client;
//!     async fn download(&self, _request: Request) -> Result<Response, SpiderError> {
//!         todo!()
//!     }
//!     fn client(&self) -> &Self::Client {
//!         &self.client
//!     }
//! }
//! ```

mod client;
mod traits;

pub use client::ReqwestClientDownloader;
pub use traits::{Downloader, HttpClient};
