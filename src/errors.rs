//! Provides all errors for the cdl-runner crate.

use glob;
use hyper;
use hyper::error::UriError;
use native_tls;
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

    ImportErr {
      description("Underlying import errror!")
      display("Underlying import error!")
    }
  }

  foreign_links {
    Globerror(glob::PatternError);
    Hypererror(hyper::Error);
    Ioerror(io::Error);
    Tlserror(native_tls::Error);
    Urierror(UriError);
  }

}
