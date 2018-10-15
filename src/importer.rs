//! Actually imports the data into a database.

use api_client::{CanvasDataApiClient, TableDefinition};
use db_client::ImportDatabaseAdapter;
use errors::*;
use flate2::read::GzDecoder;
use glob::glob;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use type_converter::convert_type_for_db;

lazy_static! {
  /// A list of tables that may not have constant IDs, or
  /// single field PKs, and as such need to be dropped/recreated
  /// on each import.
  static ref VOLATILE_TABLES: Vec<String> = vec![
    "module_completion_requirement_fact".to_owned(),
    "module_fact".to_owned(),
    "module_item_fact".to_owned(),
    "module_prerequisite_fact".to_owned(),
    "module_progression_completion_requirement_fact".to_owned(),
    "module_progression_fact".to_owned(),
    "quiz_fact".to_owned(),
    "quiz_question_answer_fact".to_owned(),
    "quiz_question_fact".to_owned(),
    "quiz_question_group_fact".to_owned(),
    "quiz_submission_fact".to_owned(),
    "quiz_submission_historical_fact".to_owned(),
    "module_completion_requirement_dim".to_owned(),
    "module_dim".to_owned(),
    "module_item_dim".to_owned(),
    "module_prerequisite_dim".to_owned(),
    "module_progression_completion_requirement_dim".to_owned(),
    "module_progression_dim".to_owned(),
    "quiz_dim".to_owned(),
    "quiz_question_answer_dim".to_owned(),
    "quiz_question_dim".to_owned(),
    "quiz_question_group_dim".to_owned(),
    "quiz_submission_dim".to_owned(),
    "quiz_submission_historical_dim".to_owned(),
    "submission_comment_participant_dim".to_owned(),
    "requests".to_owned(),
    "assignment_override_user_rollup_fact".to_owned(),
    "enrollment_rollup_dim".to_owned(),
  ];
}

/// The Root Importer Object.
pub struct Importer<T: ImportDatabaseAdapter> {
  /// The Canvas Data API Client.
  api_client: CanvasDataApiClient,
  /// The Dump ID to process.
  dump_id: String,
  /// The location of where to save stuff.
  save_location: String,
  /// The Importing Database Adapter.
  db_adapter: T,
}
unsafe impl<T: ImportDatabaseAdapter> Send for Importer<T> {}
unsafe impl<T: ImportDatabaseAdapter> Sync for Importer<T> {}

/// A representation of the filenaame.
struct FileNameSplit {
  /// The Table name of this file.
  pub table_name: String,
  /// The part of the internal shard for this file.
  pub sharded_part: String,
  /// The part of the internal hash for this file.
  pub hash_part: String,
  /// The extension for this file.
  pub extension: String,
}

impl FileNameSplit {
  /// Split a file name that has been downloaded up into pieces to match on table names, and such easier.
  ///
  /// * `split_from` - The filename to go ahead, and split.
  pub fn new(split_from: String) -> Option<Self> {
    if split_from.find("-").is_none() {
      return None;
    }
    let as_split: Vec<_> = split_from.split("-").collect();
    if as_split.len() != 3 {
      return None;
    }
    let to_split_part = as_split[2].to_owned();
    let part_with_file_extension: Vec<_> = to_split_part.split(".").collect();
    let hash_part_frd = part_with_file_extension[0].to_owned();
    let extension_frd = part_with_file_extension[1].to_owned();

    Some(FileNameSplit {
      table_name: as_split[0].to_owned(),
      sharded_part: as_split[1].to_owned(),
      hash_part: hash_part_frd,
      extension: extension_frd,
    })
  }
}

impl<T: ImportDatabaseAdapter> Importer<T> {
  /// Creates a new Importer.
  ///
  /// * `api_client` - The API Client to use.
  /// * `db_adapter` - The Database Adapter to Import Into.
  /// * `dump_id` - The Dump ID to import.
  /// * `save_location` - The Save location.
  pub fn new(api_client: CanvasDataApiClient, db_adapter: T, dump_id: String, save_location: String) -> Self {
    Importer {
      api_client: api_client,
      dump_id: dump_id,
      save_location: save_location,
      db_adapter: db_adapter,
    }
  }

  /// Gets the table info from the definition.
  ///
  /// Gets the table info we need for processing from the definition. Specifically returns the
  /// (<Column Names>, <Column Name, Column Type>) items.
  ///
  /// * `table_def` - The Table Definition.
  fn get_table_info_from_def(&self, table_def: TableDefinition) -> (Vec<String>, BTreeMap<String, String>) {
    let mut finalized_vec = Vec::new();
    let mut finalized_map = BTreeMap::new();

    for column in table_def.columns.iter() {
      finalized_vec.push(column.name.clone());
      finalized_map.insert(
        column.name.clone(),
        convert_type_for_db(column.db_type.clone(), self.db_adapter.get_db_type())
          .expect("Failed to Convert Type for DB!"),
      );
    }

    (finalized_vec, finalized_map)
  }

  /// Gets an "ID" Like column from a list of columns, and a table name.
  ///
  /// Used to automatically "guess" a primary key for a table since our methods of naming in the schema
  /// are mostly deterministic.
  ///
  /// * `table_name` - the name of the table these columns provide for.
  /// * `columns` - A Reference to the list of columns.
  fn get_id_like_column_from_columns(
    &self,
    table_name: String,
    columns: &BTreeMap<String, Option<String>>,
  ) -> Option<String> {
    debug!("Finding ID Like column for: {}", table_name);
    // Check if we have an ID Column. If so, that's what we should use.
    if columns.contains_key("id") {
      debug!("Has ID Column!");
      return Some("id".to_owned());
    } else {
      debug!("Looking up name!");
      // Other tables are labeled like assignment_fact, and have assignment_id. Handle those.
      let find_table_name_potential = table_name.rfind("_");
      if find_table_name_potential.is_some() {
        let (the_final_table_name, _) = table_name
          .split_at(find_table_name_potential.unwrap())
          .to_owned();
        debug!("Looking up: {}_id", the_final_table_name);
        if columns.contains_key(&format!("{}_id", the_final_table_name.clone())) {
          debug!("Found per table ID!");
          return Some(format!("{}_id", the_final_table_name));
        }
        let find_final_table_name_potential = the_final_table_name.rfind("_");
        if find_final_table_name_potential.is_some() {
          let (the_final_table_name_frd, _) = the_final_table_name
            .split_at(find_final_table_name_potential.unwrap())
            .to_owned();
          debug!("Looking up: {}_id", the_final_table_name_frd);
          if columns.contains_key(&format!("{}_id", the_final_table_name_frd.clone())) {
            debug!("Found per table ID!");
            return Some(format!("{}_id", the_final_table_name_frd));
          }
        }
      }
    }
    debug!("No ID Found!");
    None
  }

  /// Processes a Dump. Aka Imports it.
  pub fn process(&self, is_all_volatile: bool) -> Result<()> {
    trace!("Process Called for dump: {}", self.dump_id);

    // Download the Files for this dump.
    try!(self.api_client.download_files_for_dump(
      self.dump_id.clone(),
    ));

    // Glob to find downloaded files.
    let saved_location_glob = format!("{}/{}/*.gz", &self.save_location, &self.dump_id);
    let mut collected: Vec<_> = try!(glob(&saved_location_glob)).collect();

    // Keep a seperate have failed for our iterator, and the tables we've already dropped.
    // Don't want to drop a table multiple times.
    let has_failed = AtomicBool::from(false);

    // Drop tables first if first.
    collected.iter_mut().map(|entry| {
      // If we've already failed, skip. Don't try to keep importing.
      if has_failed.load(Ordering::Relaxed) {
        trace!("Skipping Entry: {:?} , due to failing", entry);
        return;
      }

      if let &mut Ok(ref mut path) = entry {
        let path_frd = path;
        let file_name = path_frd.file_name().unwrap().to_str().unwrap().to_owned();
        let file_name_split = FileNameSplit::new(file_name).unwrap();

        if VOLATILE_TABLES.contains(&file_name_split.table_name) || is_all_volatile {
          let drop_res = self.db_adapter.drop_table(file_name_split.table_name);
          if drop_res.is_err() {
                error!("process -> is_volatile -> drop_res -> is_err");
                error!("{:?}", drop_res.err().unwrap());
                has_failed.store(true, Ordering::Relaxed);
                return;
          }
        }
      }
    }).count();

    let _: Vec<_> = collected
      .par_iter_mut()
      .map(|entry| {
        // If we've already failed, skip. Don't try to keep importing.
        if has_failed.load(Ordering::Relaxed) {
          trace!("Skipping Entry: {:?} , due to failing", entry);
          return;
        }

        // If we have a path of a downloaded file.
        if let &mut Ok(ref mut path) = entry {
          // Get the filename of the downloaded file, and parse it since filenames are determinsitic.
          trace!("Got Path");
          let path_frd = path.clone();
          let file_name = path_frd.file_name().unwrap().to_str().unwrap().to_owned();
          let file_name_split = FileNameSplit::new(file_name).unwrap();
          trace!("Post Split!");

          // Get the table definition for the downloaded table we're looking at.
          let table_def = self.api_client.get_table_definition(
            file_name_split.table_name.clone(),
          );
          if table_def.is_err() {
            error!("process -> table_def -> is_err");
            error!("{:?}", table_def.err().unwrap());
            has_failed.store(true, Ordering::Relaxed);
            return;
          }
          let table_def = table_def.unwrap().unwrap();
          let is_volatile_table = VOLATILE_TABLES.contains(&file_name_split.table_name) || is_all_volatile;

          // Get the columns for our table.
          let (column_names, column_defs) = self.get_table_info_from_def(table_def);
          trace!("Post Table Def!");

          // Open up the file for readaing.
          let file = File::open(path_frd);
          if file.is_err() {
            error!("process -> file -> is_err");
            error!("{:?}", file.err().unwrap());
            has_failed.store(true, Ordering::Relaxed);
            return;
          }
          let mut file = file.unwrap();
          trace!("Post File Open");

          // Read the entire file into a buffer.
          // TODO: Maybe oneday switch to a buffered reader?
          let mut buffer = Vec::new();
          let res = file.read_to_end(&mut buffer);
          if res.is_err() {
            error!("process -> res -> is_err");
            error!("{:?}", res.err().unwrap());
            has_failed.store(true, Ordering::Relaxed);
            return;
          }
          trace!("Post Reader");

          // Uncompress the file.
          let mut decoder = GzDecoder::new(buffer.as_slice());
          trace!("Post Decoder Init");
          let mut finalized_string = String::new();
          let decode_res = decoder.read_to_string(&mut finalized_string);
          if decode_res.is_err() {
            error!("prcoess -> decode_res -> is_err");
            error!("{:?}", decode_res.err().unwrap());
            has_failed.store(true, Ordering::Relaxed);
            return;
          }
          trace!("Post Decode to STR");
          debug!("Decoded String: \n {:?}", finalized_string);

          // Create the table if it doesn't exist.
          let create_res = self.db_adapter.create_table(
            file_name_split.table_name.clone(),
            column_defs.clone(),
          );
          if create_res.is_err() {
            error!("prcoess -> create_res -> is_err");
            error!("{:?}", create_res.err().unwrap());
            has_failed.store(true, Ordering::Relaxed);
            return;
          }
          trace!("Post create table");

          // For each line in this file.
          for line in finalized_string.lines() {
            trace!("Processing line: [ {:?} ]", line);
            let mut columns = BTreeMap::new();
            // Split by tabs, gather all columns.
            let split_up_tsv_line: Vec<_> = line.split("\t").collect();
            for (pos, name) in column_names.iter().enumerate() {
              let mut split_up_line = Some(split_up_tsv_line[pos].to_owned());
              if split_up_line.clone().unwrap().as_str() == "\\N" {
                split_up_line = None
              }
              columns.insert(name.to_owned(), split_up_line);
            }

            trace!("Inserting Columns: [ {:?} ]", columns);

            if is_volatile_table {
              // If we're volatile don't check if it exists already, just insert.
              trace!("Is volatile table, performing insert");
              let ins_res = self.db_adapter.insert_record(
                file_name_split.table_name.clone(),
                column_defs.clone(),
                columns,
              );
              if ins_res.is_err() {
                error!("process -> for line in finalized_string -> is_volatile -> ins_res -> is_err");
                error!("{:?}", ins_res.err().unwrap());
                has_failed.store(true, Ordering::Relaxed);
                return;
              }
            } else {
              // Perform a diff if we're not volatile.
              trace!("Is not volatile performing diff.");

              // Get the ID to diff by.
              let id_like_column = self.get_id_like_column_from_columns(file_name_split.table_name.clone(), &columns);
              if id_like_column.is_none() {
                error!("Failed to find table id like column!");
                has_failed.store(true, Ordering::Relaxed);
                return;
              }
              let id_like_column = id_like_column.unwrap();
              let id_like_value = columns
                .get(&id_like_column)
                .unwrap()
                .clone()
                .unwrap()
                .to_owned();
              trace!("Performing deletion request for id like column");
              // Send delete request for that ID. on first time seeing this will be no op due to WHERE Clause.
              let del_res = self.db_adapter.drop_record(
                file_name_split.table_name.clone(),
                column_defs.clone(),
                id_like_column,
                id_like_value,
              );
              if del_res.is_err() {
                error!("Failed to drop column!");
                has_failed.store(true, Ordering::Relaxed);
                return;
              }

              // Insert the column to overwrite.
              trace!("Performing insert");
              let ins_res = self.db_adapter.insert_record(
                file_name_split.table_name.clone(),
                column_defs.clone(),
                columns,
              );
              if ins_res.is_err() {
                error!("process -> for line in finalized_string -> !is_volatile -> ins_res -> is_err");
                error!("{:?}", ins_res.err().unwrap());
                has_failed.store(true, Ordering::Relaxed);
                return;
              }
            }
            trace!("Imported Line.");
          }
        }
      })
      .collect();

    debug!("Has Failed: {}", has_failed.load(Ordering::Relaxed));

    if !has_failed.load(Ordering::Relaxed) {
      trace!("Hasn't Failed");
      Ok(())
    } else {
      trace!("Has Failed!");
      Err(ErrorKind::ImportErr.into())
    }
  }
}
