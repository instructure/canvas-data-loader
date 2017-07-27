//! Managed the type converter for Rust

use errors::*;
use settings::DatabaseType;

/// Converts a type from a name to a FRD Database Type.
///
/// Takes a type from the Canvas Data Schema API, and turns it into the name of the type
/// for the passed in database.
///
/// * `orig_type` - The Type passed in from the Canvas Data API.
/// * `db_type` - The Database type to convert into.
pub fn convert_type_for_db(orig_type: String, db_type: DatabaseType) -> Result<String> {
  match orig_type.as_str() {
    "bigint" => Ok("BIGINT".to_owned()),
    "boolean" => {
      match db_type {
        DatabaseType::Psql => Ok("BOOLEAN".to_owned()),
        DatabaseType::Mysql => Ok("VARCHAR(10)".to_owned()),
      }
    },
    "double precision" => {
      match db_type {
        DatabaseType::Psql => Ok("double precision".to_owned()),
        DatabaseType::Mysql => Ok("FLOAT(17)".to_owned()),
      }
    }
    "enum" => Ok("TEXT".to_owned()),
    "int" => Ok("INT".to_owned()),
    "integer" => Ok("INT".to_owned()),
    "text" => {
      match db_type {
        DatabaseType::Psql => Ok("TEXT".to_owned()),
        DatabaseType::Mysql => Ok("LONGTEXT".to_owned()),
      }
    }
    "timestamp" => {
      match db_type {
        DatabaseType::Psql => Ok("TIMESTAMP".to_owned()),
        DatabaseType::Mysql => Ok("DATETIME".to_owned()),
      }
    }
    "date" => Ok("DATE".to_owned()),
    "varchar" => {
      match db_type {
        DatabaseType::Psql => Ok("TEXT".to_owned()),
        DatabaseType::Mysql => Ok("LONGTEXT".to_owned()),
      }
    }
    "guid" => {
      match db_type {
        DatabaseType::Psql => Ok("TEXT".to_owned()),
        DatabaseType::Mysql => Ok("LONGTEXT".to_owned()),
      }
    }
    "datetime" => {
      match db_type {
        DatabaseType::Psql => Ok("TIMESTAMP".to_owned()),
        DatabaseType::Mysql => Ok("DATETIME".to_owned()),
      }
    }
    some_random_value => Err(
      ErrorKind::InvalidTypeToConvert(some_random_value.to_owned()).into(),
    ),
  }
}

/// Converts a Database Type into a Cast type.
///
/// Databases can't auto cast strings as other types. So we need to sometimes manually specify
/// "hey cast this string to another type". This function takes in a type of database (postgres, etc)
/// and the type of the column, and turns into a cast type, or an empty string.
///
/// * `orig_type` - The type of the column in the database.
/// * `db_type` - The Type of the Database.
pub fn get_cast_as(orig_type: String, db_type: DatabaseType) -> String {
  match db_type {
    DatabaseType::Psql => {
      match orig_type.to_lowercase().as_str() {
        "bigint" => "int8".to_owned(),
        "boolean" => "boolean".to_owned(),
        "double precision" => "double precision".to_owned(),
        "int" => "int".to_owned(),
        "timestamp" => "timestamp".to_owned(),
        _ => "".to_owned(),
      }
    },
    DatabaseType::Mysql => {
      match orig_type.to_lowercase().as_str() {
        "bigint" => "SIGNED".to_owned(),
        "int" => "SIGNED".to_owned(),
        "float(17)" => "DECIMAL(34, 17)".to_owned(),
        "datetime" => "DATETIME".to_owned(),
        "date" => "DATE".to_owned(),
        _ => "".to_owned(),
      }
    },
  }
}
