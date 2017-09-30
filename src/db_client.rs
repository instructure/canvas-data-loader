//! Provides the Database Client for the CDL Runner.
//! This will control all the connections/inserts/updates/etc.

use errors::*;
use r2d2::{Config, ManageConnection, Pool};
use std::clone::Clone;
use std::collections::BTreeMap;
use settings::{DatabaseType, Settings};
use type_converter::get_cast_as;

#[cfg(feature = "postgres_compat")]
use r2d2_postgres::{TlsMode, PostgresConnectionManager};

#[cfg(feature = "mysql_compat")]
use mysql_pool::{CreateManager, MysqlConnectionManager};

/// The Database Client Structure.
pub struct DatabaseClient<T: ManageConnection> {
  /// The Type of the Database.
  pub db_type: DatabaseType,
  /// The Underlying Connection Pool.
  underlying_pool: Pool<T>,
}

impl<T: ManageConnection> Clone for DatabaseClient<T> {
  fn clone(&self) -> DatabaseClient<T> {
    DatabaseClient {
      db_type: self.db_type.clone(),
      underlying_pool: self.underlying_pool.clone(),
    }
  }
}

/// Something the importer can use to talk to the database.
pub trait ImportDatabaseAdapter {
  /// Gets the Database Type.
  fn get_db_type(&self) -> DatabaseType;

  /// Drops a Table in the Database.
  ///
  /// * `table_name` - The Table name to Drop.
  fn drop_table(&self, table_name: String) -> Result<()>;

  /// Creates a Table in the Database.
  ///
  /// * `table_name` - The Table name to Create.
  /// * `columns` - The column definition to create <column_name, column_type>.
  fn create_table(&self, table_name: String, columns: BTreeMap<String, String>) -> Result<()>;

  /// Drops a Record in the Database.
  ///
  /// * `table_name` - The Table Name to drop from.
  /// * `column_types` - The types of columns
  /// * `column_name` - The column name to use in the WHERE clause.
  /// * `value` - The columnv value to use in the WHERE clause.
  fn drop_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    column_name: String,
    value: String,
  ) -> Result<()>;

  /// Inserts a Record into the Database.
  ///
  /// * `table_name` - The table name to insert the record into.
  /// * `columns` - The columns to insert into the table <column_name, column_value>.
  /// * `column_types` - The types of columns to use.
  fn insert_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    columns: BTreeMap<String, Option<String>>,
  ) -> Result<()>;
}

#[cfg(feature = "postgres_compat")]
impl DatabaseClient<PostgresConnectionManager> {
  /// Creates a New Database Client for Postgres.
  ///
  /// `settings` - The underlying settings object to configure ourselves with.
  pub fn new(settings: &Settings) -> Result<DatabaseClient<PostgresConnectionManager>> {
    let config = Config::default();
    let manager = PostgresConnectionManager::new(settings.get_database_url(), TlsMode::None);
    if manager.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let manager = manager.unwrap();
    let pool = Pool::new(config, manager).expect(
      "Failed to turn connection into pool. This should never happen",
    );
    Ok(DatabaseClient::<PostgresConnectionManager> {
      db_type: DatabaseType::Psql,
      underlying_pool: pool,
    })
  }
}

#[cfg(feature = "mysql_compat")]
impl DatabaseClient<MysqlConnectionManager> {
  /// Creates a New Database Client for Mysql.
  ///
  /// `settings` - The underlying settings object to configure ourselves with.
  pub fn new(settings: &Settings) -> Result<DatabaseClient<MysqlConnectionManager>> {
    let config = Config::default();
    let manager = MysqlConnectionManager::new(settings.get_database_url().as_str());
    if manager.is_err() {
      return Err(ErrorKind::MysqlErr.into());
    }
    let manager = manager.unwrap();
    let pool = Pool::new(config, manager).expect(
      "Failed to turn a connection into pool. This should never happen",
    );
    Ok(DatabaseClient::<MysqlConnectionManager> {
      db_type: DatabaseType::Mysql,
      underlying_pool: pool,
    })
  }
}

#[cfg(feature = "postgres_compat")]
impl ImportDatabaseAdapter for DatabaseClient<PostgresConnectionManager> {
  fn get_db_type(&self) -> DatabaseType {
    trace!("get_db_type was called");
    self.db_type.clone()
  }

  fn drop_table(&self, table_name: String) -> Result<()> {
    trace!("drop_table was called for: [ {} ]", table_name);
    // Get a aconnection from the pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let connection = connection.unwrap();

    // Execute drop table statement.
    let result = connection.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[]);
    if result.is_err() {
      error!("drop_table err");
      error!("{:?}", result.err().unwrap());
      return Err(ErrorKind::PostgresErr.into());
    } else {
      trace!("drop_table was successful");
      return Ok(());
    }
  }

  fn create_table(&self, table_name: String, columns: BTreeMap<String, String>) -> Result<()> {
    trace!("create_table was called for: [ {} ]", table_name);
    // Get a Connection from the underlying DB Connection Pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let connection = connection.unwrap();

    // Create the create table statement. `default` is reseverd word, so replace with
    // `_default`.
    let mut creation_string = format!("CREATE TABLE IF NOT EXISTS {} (\n", table_name);
    for (key, val) in columns.into_iter() {
      creation_string += &format!("{} {},\n", key.replace("default", "_default"), val);
    }
    // Cut off the newline + trailing comma.
    let len = creation_string.len();
    creation_string.truncate(len - 2);
    // Append final parentheses.
    creation_string += ")";
    trace!(
      "Using the following creation string: \n {}",
      creation_string
    );

    // Execute Create Table Statement.
    let result = connection.execute(&creation_string, &[]);
    if result.is_err() {
      error!("create_table err");
      error!("{:?}", result.err().unwrap());
      return Err(ErrorKind::PostgresErr.into());
    } else {
      trace!("create_table was successful!");
      return Ok(());
    }
  }

  fn drop_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    column_name: String,
    value: String,
  ) -> Result<()> {
    trace!(
      "Drop record was called for table: {} on column: {} with value: {}",
      table_name,
      column_name,
      value
    );
    // Get a Connection from the underlying pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let connection = connection.unwrap();

    // Prepare a statemtn for deleting from a table.
    let mut prepared =
      format!(
      "DELETE FROM {} WHERE {} = ",
      table_name,
      column_name.clone(),
    );
    let the_type = column_types.get(&column_name).unwrap();

    // Make sure the column gets inserted as the right type to prevent db errors.
    let cast_as = get_cast_as(the_type.to_owned(), self.db_type.clone());
    if cast_as == "" {
      prepared += &format!("{:?}", value.replace("'", "").replace("\"", "")).replace("\"", "'");
    } else {
      prepared += &format!(
        "{:?}::{}",
        value.replace("'", "").replace("\"", ""),
        cast_as
      ).replace("\"", "'");
    }

    // Execute the preapred delete statement.
    let statement = connection.execute(&prepared, &[]);
    if statement.is_err() {
      error!("drop_record err");
      error!("{:?}", statement.err().unwrap());
      return Err(ErrorKind::PostgresErr.into());
    } else {
      return Ok(());
    }
  }

  fn insert_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    columns: BTreeMap<String, Option<String>>,
  ) -> Result<()> {
    trace!("insert_record was called for table: {}", table_name);
    // Get a connection from the underlying pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let connection = connection.unwrap();

    // Create the insert into statement.
    let mut insert_string = format!("INSERT INTO {} (", table_name);
    let mut types = BTreeMap::new();

    // We need to know all the types of the keys for the INSERT INTO () VALUES ()
    for (pos, key) in columns.keys().enumerate() {
      insert_string += &format!("{},", key.replace("default", "_default"));
      types.insert(pos, column_types.get(key).unwrap().to_owned());
    }
    let mut len = insert_string.len();
    // Remove Trailing Comma.
    insert_string.truncate(len - 1);

    // Loop over actual values.
    insert_string += ") VALUES (";
    for (pos, val) in columns.values().enumerate() {
      // Handle Nulls
      if val.is_none() {
        insert_string += "NULL,";
      } else {
        let the_type = types.get(&pos).unwrap();
        // Cast the value as the right type.
        let cast_as = get_cast_as(the_type.to_owned(), self.db_type.clone());
        if cast_as == "" {
          insert_string += &format!(
            "{:?},",
            val.clone().unwrap().replace("'", "").replace("\"", "")
          ).replace("\"", "'");
        } else {
          insert_string += &format!(
            "{:?}::{},",
            val.clone().unwrap().replace("'", "").replace("\"", ""),
            cast_as
          ).replace("\"", "'");
        }
      }
    }
    len = insert_string.len();

    // Remove Trailing Comma.
    insert_string.truncate(len - 1);
    insert_string += ")";
    debug!("Insert_record string looks like: \n {}", insert_string);

    // Execute.
    let statement = connection.execute(&insert_string, &[]);
    if statement.is_err() {
      error!("insert error");
      error!("{:?}", statement.err().unwrap());
      return Err(ErrorKind::PostgresErr.into());
    } else {
      return Ok(());
    }
  }
}


#[cfg(feature = "mysql_compat")]
impl ImportDatabaseAdapter for DatabaseClient<MysqlConnectionManager> {
  fn get_db_type(&self) -> DatabaseType {
    trace!("get_db_type was called");
    self.db_type.clone()
  }

  fn drop_table(&self, table_name: String) -> Result<()> {
    trace!("drop_table was called for: [ {} ]", table_name);

    // Get connection from the underlying pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::MysqlErr.into());
    }
    let mut connection = connection.unwrap();

    // Create DropTable statement.
    let result = connection.query(&format!("DROP TABLE IF EXISTS {}", table_name));
    if result.is_err() {
      error!("drop_table err");
      error!("{:?}", result.err().unwrap());
      return Err(ErrorKind::MysqlErr.into());
    } else {
      trace!("drop_table was successful");
      return Ok(());
    }
  }

  fn create_table(&self, table_name: String, columns: BTreeMap<String, String>) -> Result<()> {
    trace!("create_table was called for: [ {} ]", table_name);
    // Get connection from the underlying pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::MysqlErr.into());
    }
    let mut connection = connection.unwrap();

    // Form Creation String. `default`, and `generated` are reserved words.
    let mut creation_string = format!("CREATE TABLE IF NOT EXISTS {} (\n", table_name);
    for (key, val) in columns.into_iter() {
      creation_string += &format!(
        "{} {},\n",
        key.replace("default", "_default").replace(
          "generated",
          "_generated",
        ),
        val
      );
    }
    let len = creation_string.len();
    // Remove Trailing newline, and comma.
    creation_string.truncate(len - 2);
    // Ensure Character set is utf8mb4.
    creation_string += ") CHARACTER SET utf8mb4";
    trace!(
      "Using the following creation string: \n {}",
      creation_string
    );

    // Execute.
    let result = connection.query(&creation_string);
    if result.is_err() {
      error!("create_table err");
      error!("{:?}", result.err().unwrap());
      return Err(ErrorKind::MysqlErr.into());
    } else {
      trace!("create_table was successful!");
      return Ok(());
    }
  }

  fn drop_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    column_name: String,
    value: String,
  ) -> Result<()> {
    trace!(
      "Drop record was called for table: {} on column: {} with value: {}",
      table_name,
      column_name,
      value
    );
    // Grab a Connection from the pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::MysqlErr.into());
    }
    let mut connection = connection.unwrap();

    // Start Preparing a Delete from statement.
    let mut prepared =
      format!(
      "DELETE FROM {} WHERE {} = ",
      table_name,
      column_name.clone(),
    );
    let the_type = column_types.get(&column_name).unwrap();

    // Cast the type correctly.
    let cast_as = get_cast_as(the_type.to_owned(), self.db_type.clone());
    if cast_as == "" {
      prepared += &format!("{:?}", value.replace("'", "").replace("\"", "")).replace("\"", "'");
    } else {
      prepared += &format!(
        "CAST({:?} as {})",
        value.replace("'", "").replace("\"", ""),
        cast_as
      ).replace("\"", "'");
    }

    // Execute.
    let statement = connection.query(&prepared);
    if statement.is_err() {
      error!("drop_record err");
      error!("{:?}", statement.err().unwrap());
      return Err(ErrorKind::MysqlErr.into());
    } else {
      return Ok(());
    }
  }

  fn insert_record(
    &self,
    table_name: String,
    column_types: BTreeMap<String, String>,
    columns: BTreeMap<String, Option<String>>,
  ) -> Result<()> {
    trace!("insert_record was called for table: {}", table_name);
    // Get connection from the underlying pool.
    let connection = self.underlying_pool.get();
    if connection.is_err() {
      return Err(ErrorKind::PostgresErr.into());
    }
    let mut connection = connection.unwrap();

    // Start Preparing insert into statements.
    let mut insert_string = format!("INSERT INTO {} (", table_name);
    let mut types = BTreeMap::new();

    // We need the types for INSERT INTO () VALUES (). Get Those.
    for (pos, key) in columns.keys().enumerate() {
      insert_string += &format!(
        "{},",
        key.replace("default", "_default").replace(
          "generated",
          "_generated",
        )
      );
      types.insert(pos, column_types.get(key).unwrap().to_owned());
    }
    let mut len = insert_string.len();
    // Remove trailing comma.
    insert_string.truncate(len - 1);

    // Start Inserting Values.
    insert_string += ") VALUES (";
    for (pos, val) in columns.values().enumerate() {
      if val.is_none() {
        // Handle NULLs.
        insert_string += "NULL,";
      } else {
        let the_type = types.get(&pos).unwrap();
        // Cast the type correctly.
        let cast_as = get_cast_as(the_type.to_owned(), self.db_type.clone());
        if cast_as == "" {
          insert_string += &format!(
            "{:?},",
            val.clone().unwrap().replace("'", "").replace("\"", "")
          ).replace("\"", "'");
        } else {
          insert_string += &format!(
            "CAST({:?} AS {}),",
            val.clone().unwrap().replace("'", "").replace("\"", ""),
            cast_as
          ).replace("\"", "'");
        }
      }
    }
    len = insert_string.len();
    // Remove trailing commas.
    insert_string.truncate(len - 1);
    insert_string += ")";
    debug!("Insert_record string looks like: \n {}", insert_string);

    // Execute.
    let statement = connection.query(&insert_string);
    if statement.is_err() {
      error!("insert error");
      error!("{:?}", statement.err().unwrap());
      return Err(ErrorKind::MysqlErr.into());
    } else {
      return Ok(());
    }
  }
}
