//! Provides all errors for the cdl-runner crate.

use glob;
use reqwest;
use std::io;

error_chain! {

  errors {
    InvalidTypeToConvert(the_type: String) {
      description("Cannot convert type to a Database Type!")
      display("Invalid Type: [ {} ] to convert to DB", the_type)
    }

    PostgresErr {
      description("Underlying postgres error!")
      display("Underlying postgres error!")
    }

    MysqlErr {
      description("Underlying Mysql error!")
      display("Underlying Mysql error!")
    }

    ImportErr {
      description("Underlying import errror!")
      display("Underlying import error!")
    }
  }

  foreign_links {
    Globerror(glob::PatternError);
    HttpError(reqwest::Error);
    HttpUrlError(reqwest::UrlError);
    Ioerror(io::Error);
  }

}
