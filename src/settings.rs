//! Provides the `Settings` Struct for the rest of the crate in order to get
//! configuration values from the environment, or one of several files.

use config::{Config, File, Environment};

/// An Enum of all possible database types.
///
/// Contains a list of all possible database types that the loader supports.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub enum DatabaseType {
  /// A type for postgres-like databases.
  Psql,
  /// A type for mysql-like databases.
  Mysql,
}

/// The Database Configuration object.
///
/// Handles all database configuration values, which in this case is just the connection URL.
#[derive(Debug, Deserialize)]
struct Database {
  /// The connection URL for the Database.
  pub url: String,
  /// The Type of The Database.
  pub db_type: String,
}

/// The Canvas Data API Auth Configuration object.
///
/// Handles all the configuration values for the Canvas Data API. In this case just the
/// api key + api secrete for Canvas Data.
#[derive(Debug, Deserialize)]
struct Canvasdataauth {
  /// The API Key for Canvas Data.
  pub api_key: String,
  /// The API Secret for Canvas Data.
  pub api_secret: String,
}

/// The Global Settings object for all configuration values.
#[derive(Debug, Deserialize)]
pub struct Settings {
  /// The database configuration object.
  database: Database,
  /// The Canvas Data API Auth Configuration Object.
  canvasdataauth: Canvasdataauth,
  /// The place to save files.
  save_location: String,
  /// The place to store the Rocks DB Database.
  rocksdb_location: String,
  /// Whether or not to skip historical imports.
  skip_historical_imports: bool,
  /// Only attempts to load the latest import.
  only_load_final: Option<bool>,
}

impl Settings {
  /// Creates a new settings object.
  pub fn new() -> Self {
    let mut base_configuration = Config::new();
    base_configuration
      .merge(File::with_name("config/default"))
      .expect("Could not find default configuration file");

    base_configuration
      .merge(File::with_name("config/local").required(false))
      .expect("Transient error getting local configuration.");

    base_configuration
      .merge(Environment::with_prefix("cdl"))
      .expect("Transient error getting environment variables");

    base_configuration.try_into().expect(
      "Failed to create base configuration",
    )
  }

  /// Gets the save location provided by the settings.
  pub fn get_save_location(&self) -> String {
    self.save_location.clone()
  }

  /// Gets the rocksdb location provided by the settings.
  pub fn get_rocksdb_location(&self) -> String {
    self.rocksdb_location.clone()
  }

  /// Gets the notion of whether or not to skip historical imports from the settings.
  pub fn get_should_skip_historical_imports(&self) -> bool {
    self.skip_historical_imports
  }

  /// Gets the notion of whether or not to only load the final import.
  pub fn get_should_only_load_final(&self) -> bool {
    self.only_load_final.unwrap_or(false)
  }

  /// Gets the database url provided by the settings.
  pub fn get_database_url(&self) -> String {
    self.database.url.clone()
  }

  /// Gets the database type provided by the settings.
  pub fn get_database_type(&self) -> DatabaseType {
    match self.database.db_type.to_lowercase().as_str() {
      "mysql" => DatabaseType::Mysql,
      _ => DatabaseType::Psql,
    }
  }

  /// Gets the Canvas Data API Key provided by the settings.
  pub fn get_canvas_data_api_key(&self) -> String {
    self.canvasdataauth.api_key.clone()
  }

  /// Gets the Canvas Data API Secret provided by the settings.
  pub fn get_canvas_data_api_secret(&self) -> String {
    self.canvasdataauth.api_secret.clone()
  }
}
