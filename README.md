# Canvas Data Loader #

This is the source code for the Canvas Data Loader. The Canvas Data Loader is an example application
that downloads your data, and imports it into a Database. The process is completely automated, and is
able to handle things like Historical Refreshes, Schema Changes, and the 24-36 hour variance all without
issue.

It should be noted although there are better options out there, there isn't a reason why you couldn't use
the loader to handle all of your imports everyday. The Canvas Data Loader could for example handle your
imports at first, and then later off be handed to a more stable process.

## Support ##

Although this is under the Instructure Repo this is purely an example application, and as such is not fully supported by Instructure.

However, the Canvas Data Support team is happy to field requests about usage in the standard canvasdatahelp@instructure.com email.

## How Do I Use It? ##

The following instructions are for a linux server, but steps 1-5 should work universally.
You'll just need to use your systems way of scheduling a repeating task instead of crons if you
are not using linux.

* Clone this repository.
* Copy the default configuration, and modify it to your needs:
  * `cp ./config/default.toml ./config/local.toml`
  * `my_text_editor ./config/local.toml`
* Choose a home for the importer, and copy this repository there.
* [Install Rust](https://www.rust-lang.org/en-US/install.html)
* Build a release version: `cargo build --release`.
* Setup a crontab to run the importer every hour:
  * `crontab -e`
  * Enter on it's own line, replacing the path to your importer: `0 * * * * cd <my_cdl_location> && RUST_LOG=info ./target/release/cdl-runner > /var/log/cdl-log 2>&1`
* Tadah!

### Configuration Using Environment Variables

Configuration can also be done using environment variables instead of, or in addition to the `./config/local.toml` file. For example, you may wish to use environment variables for the API key/secret and use the file for the remaining configuration.

Example:

`export cdl__canvasdataauth__api_key=abcdefg123456`
`export cdl__canvasdataauth__api_secret=123456abcdefg`

Possible environment variables:

- `cdl__canvasdataauth__api_key`
- `cdl__canvasdataauth__api_secret`
- `cdl__database__db_type`
- `cdl__database__url` 
- `cdl__only_load_final`
- `cdl__rocksdb_location`
- `cdl__save_location`
- `cdl__skip_historical_imports`

## License ##

The Canvas Data Loader is Licensed under MIT.
