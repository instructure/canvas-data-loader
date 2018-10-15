//! Provides an API Client for the Canvas Data API.

use base64::encode as B64Encode;
use chrono::prelude::*;
use errors::*;
use rayon::prelude::*;
use regex::Regex;
use reqwest::{Client as HttpClient, Method, Request};
use reqwest::header::HeaderValue;
use ring::{digest, hmac};
use settings::Settings;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;

lazy_static! {
  static ref REQREG: Regex = Regex::new(r"^requests.*?$").expect("Invalid Static Requests Regex");
}

/// The API Client for Canvas Data.
#[derive(Clone)]
pub struct CanvasDataApiClient {
  /// The API Key to use for Canvas Data.
  api_key: String,
  /// The API Secret to use for Canvas Data.
  api_secret: String,
  /// The place to save files.
  save_location: String,
  /// The Reqwest Client,
  client: HttpClient,
}

impl CanvasDataApiClient {
  /// Creates a new Canvas Data API Client.
  ///
  /// Creates a Canvas Data API Client that talks to the core portal.inshosteddata.com.
  ///
  /// * `settings` - The settings to use for this API Client.
  pub fn new(settings: &Settings) -> Self {
    CanvasDataApiClient {
      api_key: settings.get_canvas_data_api_key(),
      api_secret: settings.get_canvas_data_api_secret(),
      save_location: settings.get_save_location(),
      client: HttpClient::new(),
    }
  }

  /// Computes the authorization header.
  ///
  /// Computes the authorization header needed for authenticating to the Canvas Data API.
  ///
  /// * `http_method` - The HTTP Method you're using.
  /// * `host` - The Host Header you're using.
  /// * `content_type` - The Content Type you're using.
  /// * `content_md5` - The Content MD5 Header you're sending.
  /// * `path` - The path of your request.
  /// * `query_params` - The query parameters of your request.
  /// * `date_header` - The Date Header you're using.
  pub fn compute_auth_header(
    &self,
    http_method: &str,
    host: &str,
    content_type: &str,
    content_md5: &str,
    path: &str,
    query_params: &str,
    date_header: &str,
  ) -> String {

    let pre_sign =
      format!(
      "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
      http_method,
      host,
      content_type,
      content_md5,
      path,
      query_params,
      date_header,
      self.api_secret,
    );
    debug!("Compute Auth Header was passed: {:?}", pre_sign);

    let signing_key = hmac::SigningKey::new(&digest::SHA256, self.api_secret.clone().as_bytes());
    let output = hmac::sign(&signing_key, pre_sign.as_bytes());
    let encoded_val = B64Encode(&output);
    format!("HMACAuth {}:{}", self.api_key, encoded_val)
  }

  /// Gets the current date.
  ///
  /// Gets the current date in the format needed for compute_auth_header.
  pub fn get_current_date(&self) -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
  }

  /// Determines if a dump is a historical refresh based on the files in dump response.
  ///
  /// * `resp` - The Files in dump response to check.
  pub fn is_historical_refresh(&self, resp: FilesInDumpResponse) -> bool {
    let mut has_found_all_requests_table = true;
    'outer: for artifact in resp.artifacts_by_table.values() {
      for file in artifact.files.iter() {
        if !REQREG.is_match(&file.filename) {
          has_found_all_requests_table = false;
          break 'outer;
        }
      }
    }
    has_found_all_requests_table
  }

  /// Gets a current list of Dumps for your Canvas Data Instance.
  pub fn get_dumps(&self) -> Result<Vec<DumpInList>> {
    trace!("Get Dumps was called.");

    let uri = try!("https://portal.inshosteddata.com/api/account/self/dump".parse());
    let mut req: Request = Request::new(Method::GET, uri);
    let date_str = self.get_current_date();
    req.headers_mut().insert("Date", HeaderValue::from_str(&date_str).expect("Couldn't turn string into header value!"));
    req.headers_mut().insert(
      "Content-Type",
      HeaderValue::from_static("application/json"),
    );
    req.headers_mut().insert(
      "Authorization",
      HeaderValue::from_str(&self.compute_auth_header(
        "GET",
        "portal.inshosteddata.com",
        "application/json",
        "",
        "/api/account/self/dump",
        "",
        &date_str,
      )).expect("Couldn't turn string into header value!"),
    );

    Ok(try!(self.client.execute(req).and_then(|mut res| {
      res.json()
    }).map_err(|e| {
      io::Error::new(io::ErrorKind::Other, e)
    })))
  }

  /// Gets the latest schema.
  pub fn get_latest_schema(&self) -> Result<SchemaDefinition> {
    trace!("Get latest schema was called");

    let uri = try!("https://portal.inshosteddata.com/api/schema/latest".parse());
    let mut req: Request = Request::new(Method::GET, uri);
    let date_str = self.get_current_date();
    req.headers_mut().insert("Date", HeaderValue::from_str(&date_str).expect("Failed to turn string into header value!"));
    req.headers_mut().insert(
      "Content-Type",
      HeaderValue::from_static("application/json"),
    );
    req.headers_mut().insert(
      "Authorization",
      HeaderValue::from_str(&self.compute_auth_header(
        "GET",
        "portal.inshosteddata.com",
        "application/json",
        "",
        "/api/schema/latest",
        "",
        &date_str,
      )).expect("Failed to turn string into header value!"),
    );

    Ok(try!(self.client.execute(req).and_then(|mut res| {
      res.json()
    }).map_err(|e| {
      io::Error::new(io::ErrorKind::Other, e)
    })))
  }

  /// Gets the Table Definition for a Specific Table.
  ///
  /// * `table_name` - The Table name to get the definition for.
  pub fn get_table_definition(&self, table_name: String) -> Result<Option<TableDefinition>> {
    trace!("get_table_definition was called for: [ {} ]", table_name);

    let uri = try!("https://portal.inshosteddata.com/api/schema/latest".parse());
    let mut req: Request = Request::new(Method::GET, uri);
    let date_str = self.get_current_date();
    req.headers_mut().insert("Date", HeaderValue::from_str(&date_str).expect("Failed to turn string into headervalue!"));
    req.headers_mut().insert(
      "Content-Type",
      HeaderValue::from_static("application/json"),
    );
    req.headers_mut().insert(
      "Authorization",
      HeaderValue::from_str(&self.compute_auth_header(
        "GET",
        "portal.inshosteddata.com",
        "application/json",
        "",
        "/api/schema/latest",
        "",
        &date_str,
      )).expect("Failed to turn string into headervalue!"),
    );

    Ok(try!(self.client.execute(req).and_then(|mut res| {
      res.json()
    }).map(|res: SchemaDefinition| {
      let mut ret = None;

      for table_def in res.schema.values() {
        if table_def.table_name.to_lowercase() == table_name {
          ret = Some(table_def.clone());
          break;
        }
      }

      ret
    }).map_err(|e| {
      io::Error::new(io::ErrorKind::Other, e)
    })))
  }

  /// Gets the list of files for a specific dump.
  ///
  /// * `dump_id` - The Dump ID to grab the list of files for.
  pub fn get_files_for_dump(&self, dump_id: String) -> Result<FilesInDumpResponse> {
    trace!(
      "Get files for dump was called with dump id: [ {} ]",
      dump_id
    );

    let path = format!("/api/account/self/file/byDump/{}", dump_id);
    let uri = try!(format!("https://portal.inshosteddata.com{}", &path).parse());
    let mut req: Request = Request::new(Method::GET, uri);
    let date_str = self.get_current_date();
    req.headers_mut().insert("Date", HeaderValue::from_str(&date_str).expect("Failed to turn string into header value!"));
    req.headers_mut().insert(
      "Content-Type",
      HeaderValue::from_static("application/json"),
    );
    req.headers_mut().insert(
      "Authorization",
      HeaderValue::from_str(&self.compute_auth_header(
        "GET",
        "portal.inshosteddata.com",
        "application/json",
        "",
        &path,
        "",
        &date_str,
      )).expect("Failed to turn string into headervalue!"),
    );

    Ok(try!(self.client.execute(req).and_then(|mut res| {
      res.json()
    }).map_err(|e| {
      io::Error::new(io::ErrorKind::Other, e)
    })))
  }

  /// Download all files for a specific dump.
  ///
  /// * `dump_id` - The Dump ID of the files to download.
  pub fn download_files_for_dump(&self, dump_id: String) -> Result<()> {
    trace!(
      "Download files for dump was called with dump id: [ {} ]",
      dump_id
    );
    let save_location = format!("{}/{}", self.save_location, &dump_id);
    try!(fs::create_dir_all(save_location.clone()));
    let files_in_dump = try!(self.get_files_for_dump(dump_id.clone()));

    files_in_dump.artifacts_by_table.par_iter().map(move |(_, table_artifact)| {
      for file_to_download in table_artifact.files.iter().cloned() {
        let finalized_to_download_path = format!("{}/{}", &save_location, &file_to_download.filename);
        let cloned_download_path = finalized_to_download_path.clone();
        let path = Path::new(&finalized_to_download_path);
        if path.exists() {
          debug!(
            "{:?} exists, skipping entire artifact",
            cloned_download_path
          );
          // Assume the entire artifact is downloaded.
          continue;
        } else {
          debug!(
            "{:?} does not exist, downloading files",
            cloned_download_path
          );
          let uri = file_to_download.url.parse().expect("Failed to parse file url form hosted-data!");
          let req = Request::new(Method::GET, uri);
          self.client.execute(req).map(move |mut res| {
            let download_path = cloned_download_path;
            let mut file = File::create(Path::new(&download_path)).expect("Failed to create download file!");

            res.copy_to(&mut file).expect("Failed to copy to file!")
          }).expect("Failed to download table!");
        }
      }
    }).count();

    trace!("Done Downloading Files for: {}", dump_id);

    Ok(())
  }
}

/// Represents a Dump returned from the list dumps endpoint.
#[derive(Clone, Debug, Deserialize)]
pub struct DumpInList {
  /// The ID of this particular Dumpm.
  #[serde(rename = "dumpId")]
  pub dump_id: String,
  /// The Sequence number of this dump.
  pub sequence: i64,
  /// The Account ID this dump is for.
  #[serde(rename = "accountId")]
  pub account_id: String,
  /// The Number of Files this dump is reporting.
  #[serde(rename = "numFiles")]
  pub num_files: i64,
  /// If this dump is finished.
  pub finished: bool,
  /// When this dump is set to expire.
  pub expires: i64,
  /// When this dump was last updated.
  #[serde(rename = "updatedAt")]
  pub updated_at: DateTime<Utc>,
  /// When this dump was created.
  #[serde(rename = "createdAt")]
  pub created_at: DateTime<Utc>,
  /// The Schema Version this dump is using.
  #[serde(rename = "schemaVersion")]
  pub schema_version: String,
}
unsafe impl Send for DumpInList {}

/// The list of files returned from a file in dump response.
#[derive(Clone, Debug, Deserialize)]
pub struct FilesInDumpResponse {
  /// The Account ID these files are for.
  #[serde(rename = "accountId")]
  pub account_id: String,
  /// When these files expire.
  pub expires: i64,
  /// The sequence of the dump these files are apart of.
  pub sequence: i64,
  /// When these files were last updated.
  #[serde(rename = "updatedAt")]
  pub updated_at: DateTime<Utc>,
  /// The schema version these files are at.
  #[serde(rename = "schemaVersion")]
  pub schema_version: String,
  /// The number of files that exist.
  #[serde(rename = "numFiles")]
  pub num_files: i64,
  /// When the dump was created these files are apart of.
  #[serde(rename = "createdAt")]
  pub created_at: DateTime<Utc>,
  /// The Dump ID these files are related to.
  #[serde(rename = "dumpId")]
  pub dump_id: String,
  /// Whether the dump is finished or not.
  pub finished: bool,
  /// A list of the "artifacts" or files per table.
  #[serde(rename = "artifactsByTable")]
  pub artifacts_by_table: BTreeMap<String, ArtifactByTable>,
}
unsafe impl Send for FilesInDumpResponse {}

/// A list of artifacts per table.
#[derive(Clone, Debug, Deserialize)]
pub struct ArtifactByTable {
  /// The table name these artifacts are apart of.
  #[serde(rename = "tableName")]
  pub table_name: String,
  /// Whether or not this is a partial table.
  pub partial: bool,
  /// A List of files for this table.
  pub files: Vec<BasicFile>,
}
unsafe impl Send for ArtifactByTable {}

/// A File object returned in ArtifactsByTable.
#[derive(Clone, Debug, Deserialize)]
pub struct BasicFile {
  /// The URL for this file to download from.
  pub url: String,
  /// The filename of this file.
  pub filename: String,
}
unsafe impl Send for BasicFile {}

/// The Schema Definition returned by Canvas Data.
#[derive(Clone, Debug, Deserialize)]
pub struct SchemaDefinition {
  /// The Version of the schema.
  pub version: String,
  /// The Actual Schema Object itself.
  pub schema: BTreeMap<String, TableDefinition>,
}
unsafe impl Send for SchemaDefinition {}

/// A Definition for a Table returned by the Schema API.
#[derive(Clone, Debug, Deserialize)]
pub struct TableDefinition {
  /// The DW Type (dimension, or fact).
  pub dw_type: String,
  /// An optional Description of the table.
  pub description: Option<String>,
  /// Any hints about how a table, almost always empty, may occasionally provide a sort key, or something of the like.
  pub hints: BTreeMap<String, String>,
  /// Whther this table is incremental.
  pub incremental: bool,
  /// The table name of this table,
  #[serde(rename = "tableName")]
  pub table_name: String,
  /// A List of it's columns.
  pub columns: Vec<ColumnDefinition>,
}
unsafe impl Send for TableDefinition {}

/// A Definition for a Column returned by the Schema API.
#[derive(Clone, Debug, Deserialize)]
pub struct ColumnDefinition {
  /// The Type this column is.
  #[serde(rename = "type")]
  pub db_type: String,
  /// An optional description of this column.
  pub description: Option<String>,
  /// The name of this column/
  pub name: String,
  /// An optional length to apply to this column.
  pub length: Option<i64>,
  /// Optional information about the dimension.
  pub dimension: Option<DimensionDefinition>,
}
unsafe impl Send for ColumnDefinition {}

/// Dimension information returned by the Schema API.
#[derive(Clone, Debug, Deserialize)]
pub struct DimensionDefinition {
  /// The name of this dimension.
  pub name: String,
  /// The ID of this dimension.
  pub id: String,
  /// An optional role to attach to this dimension.
  pub role: Option<String>,
}
unsafe impl Send for DimensionDefinition {}
