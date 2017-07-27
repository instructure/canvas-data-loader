extern crate base64;
extern crate chrono;
extern crate config;
#[macro_use]
extern crate error_chain;
extern crate env_logger;
extern crate flate2;
extern crate futures;
extern crate glob;
extern crate hyper;
extern crate hyper_tls;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate native_tls;
extern crate r2d2;
extern crate regex;
extern crate ring;
extern crate rocksdb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;

#[cfg(feature = "postgres_compat")]
extern crate postgres;
#[cfg(feature = "postgres_compat")]
extern crate r2d2_postgres;

#[cfg(feature = "mysql_compat")]
extern crate mysql;

pub mod api_client;
pub mod db_client;
pub mod errors;
pub mod importer;
pub mod settings;
pub mod type_converter;

#[cfg(feature = "mysql_compat")]
pub mod mysql_pool;

use db_client::DatabaseClient;
use rocksdb::DB;
use settings::DatabaseType;

#[cfg(feature = "postgres_compat")]
use r2d2_postgres::PostgresConnectionManager;

#[cfg(feature="mysql_compat")]
use mysql_pool::MysqlConnectionManager;

/// Entry Point to the application.
fn main() {
  env_logger::init().expect("Failed to setup logger!");

  let settings = settings::Settings::new();
  info!("Setting up API Client...");
  let api_client = api_client::CanvasDataApiClient::new(&settings);
  let mut dumps = api_client.get_dumps().expect("Failed to get List of Dumps");
  dumps.sort_by(|dump_one, dump_two| {
    dump_one.created_at.cmp(&dump_two.created_at)
  });
  println!("{:?}", dumps);
  info!("Connecting to RocksDB Store....");
  let whiskey = DB::open_default(settings.get_rocksdb_location()).expect("Failed to open RocksDB");
  let latest_schema = api_client.get_latest_schema().expect(
    "Failed to fetch latest schema!",
  );
  let has_errord = false;
  let _: Vec<_> = dumps
    .into_iter()
    .map(|dump| {
      if has_errord {
        info!("Skipping dump: {} due to previous failure in import", dump.dump_id);
        return Err(());
      }
      debug!("Entering debug loop for dump: {}", dump.dump_id);
      if !dump.finished {
        info!("Skipping dump: {} because it's not finished.", dump.dump_id);
        return Ok(());
      }
      let result = whiskey.get(
        format!("dump_processed_{}", dump.dump_id.clone()).as_bytes(),
      );
      if result.is_err() {
        error!("Failed to get value from Rocks!");
        error!("{:?}", result.err().unwrap());
        return Err(());
      }
      let is_potentially_processed = result.unwrap();
      if is_potentially_processed.is_some() {
        let potentially_processed = is_potentially_processed.unwrap();
        let potentially_processed = potentially_processed.to_utf8();
        if potentially_processed.is_some() {
          let processed = potentially_processed.unwrap();
          if processed == "successful" || processed == "out-of-date" {
            info!("Skipping already processed dump: {}", dump.dump_id);
            return Ok(());
          }
        }
      }
      if latest_schema.version != dump.schema_version {
        let _ = whiskey.put(
          format!("dump_processed_{}", dump.dump_id.clone()).as_bytes(),
          b"out-of-date",
        );
        return Ok(());
      }
      let files_in_dump = api_client.get_files_for_dump(dump.dump_id.clone());
      if files_in_dump.is_err() {
        info!("Failed to list files for dump. Skipping...");
        return Ok(());
      }
      let files_in_dump = files_in_dump.unwrap();
      if api_client.is_historical_refresh(files_in_dump) && settings.get_should_skip_historical_imports() {
        info!("Skipping dump: {} since it's a historical refresh", dump.dump_id.clone());
        let _ = whiskey.put(
          format!("dump_processed_{}", dump.dump_id.clone()).as_bytes(),
          b"successful",
        );
        return Ok(());
      }
      let _ = whiskey.put(
        format!("dump_processed_{}", dump.dump_id.clone()).as_bytes(),
        b"in_progress",
      );
      if cfg!(feature = "postgres_compat") {
        if settings.get_database_type() == DatabaseType::Psql {
          info!("Connecting to the DB");
          let db_client = db_client::DatabaseClient::<PostgresConnectionManager>::new(&settings)
                  .expect("Couldn't setup DB Client");
          let importer = importer::Importer::<DatabaseClient<PostgresConnectionManager>>::new(
            api_client.clone(),
            db_client,
            dump.dump_id.clone(),
            settings.get_save_location(),
          );
          let res = importer.process();
          if res.is_ok() {
            let _ = whiskey.put(
              format!("dump_processed_{}", dump.dump_id).as_bytes(),
              b"successful",
            );
            return Ok(());
          } else {
            let _ = whiskey.put(
              format!("dump_processed_{}", dump.dump_id).as_bytes(),
              b"failure",
            );
            return Err(());
          }
        }
      }
      if cfg!(feature = "mysql_compat") {
        if settings.get_database_type() == DatabaseType::Mysql {
          info!("Connecting to the DB");
          let db_client = db_client::DatabaseClient::<MysqlConnectionManager>::new(&settings)
                  .expect("Couldn't setup DB Client");
          let importer = importer::Importer::<DatabaseClient<MysqlConnectionManager>>::new(
            api_client.clone(),
            db_client,
            dump.dump_id.clone(),
            settings.get_save_location(),
          );
          let res = importer.process();
          if res.is_ok() {
            let _ = whiskey.put(
              format!("dump_processed_{}", dump.dump_id).as_bytes(),
              b"successful",
            );
            return Ok(());
          } else {
            let _ = whiskey.put(
              format!("dump_processed_{}", dump.dump_id).as_bytes(),
              b"failure",
            );
            return Err(());
          }
        }
      }
      Err(())
    })
    .collect();
  info!("Done!");
}
