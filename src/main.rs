use std::{
  fs::{File, OpenOptions},
  io::Write,
  time::Duration,
};

use ansi_term::Color;
use chrono::Local;
use clap::Parser;
use env_logger::Builder;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info, Level, LevelFilter};
use sqlx::{Column, Row};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
  /// host name
  #[arg(
    short,
    long,
    value_parser,
    value_name = "host",
    default_value = "localhost",
    help = "Sets the database host"
  )]
  host: String,

  /// port
  #[arg(
    short,
    long,
    value_parser,
    value_name = "port",
    default_value = "3306",
    help = "Sets the database port"
  )]
  port: String,

  /// username
  #[arg(
    short,
    long,
    value_parser,
    value_name = "username",
    default_value = "root",
    help = "Sets the database user"
  )]
  username: String,

  /// password
  #[arg(
    short,
    long,
    value_parser,
    value_name = "password",
    default_value = "123456",
    help = "Sets the database password"
  )]
  password: String,

  /// database name
  #[arg(
    short,
    long,
    value_parser,
    value_name = "database",
    help = "Sets the database name"
  )]
  db: String,

  /// table name
  #[arg(
    short,
    long,
    value_parser,
    value_name = "table",
    help = "Sets the table name"
  )]
  table: String,

  /// unique index
  #[arg(
    short,
    long,
    value_parser,
    value_name = "index",
    default_value = "",
    help = "The primary key of the data table"
  )]
  index: Option<String>,

  /// delimiter
  #[arg(
    short,
    long,
    value_parser,
    value_name = "delimiter",
    default_value = "|",
    help = "Write delimiter to CSV file"
  )]
  delim: String,

  /// sql script
  #[arg(
    short,
    long,
    value_parser,
    value_name = "sql",
    help = "The SQL query script"
  )]
  sql: String,

  /// replace column
  #[arg(
    short,
    long,
    value_parser,
    value_name = "replcol",
    default_value = "",
    help = "The column that needs special handling"
  )]
  repcol: String,

  /// output path
  #[arg(
    short,
    long,
    value_parser,
    value_name = "output",
    default_value = "./output",
    help = "The output path for saving files"
  )]
  output: String,
}

pub async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
  let url = format!(
    "mysql://{}:{}@{}:{}/{}",
    cli.username, cli.password, cli.host, cli.port, cli.db
  );

  info!("Connecting to MySQL database...");

  let pool: sqlx::Pool<sqlx::MySql> = match sqlx::MySqlPool::connect(&url).await {
    Ok(pool) => pool,
    Err(err) => {
      error!("connect mysql error: {}", err);
      return Err(Box::new(err));
    }
  };
  if !folder_exists(&cli.output) {
    std::fs::create_dir(&cli.output)?;
  }
  info!("Creating logs.log file...");
  File::create(format!("{}/logs.log", cli.output)).expect("Failed to create file");
  let mut log_file = OpenOptions::new()
    .append(true)
    .open(format!("{}/logs.log", cli.output))?;
  let check_msg = format!("Checking {}, please wait...", cli.table);
  info!(
    "Checking {}, and creating output directory if not exists...",
    cli.table
  );
  let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
  let check_msg_log = format!("{} => {}\n", &timestamp, &check_msg);
  log_file.write_all(check_msg_log.as_bytes())?;

  // query headers
  info!("Fetching table headers...");
  let re = regex::Regex::new(r"(?i)\blimit\s+\d+(\s*,\s*\d+)?\b.*")?;
  let header_q = re.replace_all(&cli.sql, "").into_owned();
  let sql_query_header = format!("{} LIMIT 10", header_q);
  info!("Executing main SQL query...");
  match sqlx::query(&sql_query_header).fetch_one(&pool).await {
    Ok(rows) => {
      let col_num = rows.columns().len();
      let mut vec_col_name: Vec<&str> = Vec::new();
      let mut vec_col_type: Vec<String> = Vec::new();
      for num in 0..col_num {
        vec_col_name.push(rows.column(num).name());
        vec_col_type.push(rows.column(num).type_info().to_string());
      }

      // execute query
      let mut stream = sqlx::query(&cli.sql).fetch(&pool);

      let total_rows = match &cli.index {
        None => {
          let count_query = format!("select count(*) from {}", cli.table);
          let row_count: (i64,) = sqlx::query_as(&count_query).fetch_one(&pool).await?;
          row_count.0 as usize
        }
        Some(index) => {
          if vec_col_name.contains(&index.as_str()) {
            let max_id_query = format!("select max({}) from {}", index, cli.table);
            let max_id: i64 = sqlx::query_scalar(&max_id_query).fetch_one(&pool).await?;
            max_id as usize
          } else {
            let count_query = format!("select count(*) from {}", cli.table);
            let row_count: (i64,) = sqlx::query_as(&count_query).fetch_one(&pool).await?;
            row_count.0 as usize
          }
        }
      };

      let pb = ProgressBar::new(total_rows as u64);
      pb.enable_steady_tick(Duration::from_millis(100));
      pb.set_style(
        ProgressStyle::default_bar()
          .template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/orange}] {pos}/{len} ({eta})",
          )?
          .progress_chars("=>-"),
      );

      let emit_msg = format!("{}", cli.table);
      let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
      let check_done_log = format!("{} => {}\n", &timestamp, &emit_msg);
      log_file.write_all(check_done_log.as_bytes())?;

      let folder_path = format!("{}/{}", cli.output, cli.table);

      if !folder_exists(&folder_path) {
        std::fs::create_dir(&folder_path)?;
      }

      // save path
      let output_path = format!("{}/{}.csv", &folder_path, cli.table);
      let delim = cli.delim.as_bytes().first().cloned().unwrap_or(b'|');
      let mut wtr = csv::WriterBuilder::new()
        .delimiter(delim)
        .from_path(output_path)?;

      // write headers
      wtr.serialize(vec_col_name.clone())?;
      while let Some(row) = stream.try_next().await? {
        let mut vec_wtr_str = Vec::new();
        for num in 0..col_num {
          let value = match &vec_col_type[num][..] {
            "DECIMAL" => {
              let num: rust_decimal::Decimal = row.get(num);
              num.to_string()
            }
            "DOUBLE" => {
              let num: f64 = row.get(num);
              num.to_string()
            }
            "FLOAT" => {
              let num: f32 = row.get(num);
              num.to_string()
            }
            "SMALLINT" | "TINYINT" => {
              let num: i16 = row.get(num);
              num.to_string()
            }
            "INT" | "MEDIUMINT" | "INTEGER" => {
              let num: i32 = row.get(num);
              num.to_string()
            }
            "BIGINT" => {
              let num: i64 = row.get(num);
              num.to_string()
            }
            "INT UNSIGNED" => {
              let num: u32 = row.get(num);
              num.to_string()
            }
            "DATETIME" => {
              let num: chrono::DateTime<chrono::Local> = row.get(num);
              num.to_string()
            }
            "DATE" => {
              let num: sqlx::types::time::Date = row.get(num);
              num.to_string()
            }
            "BOOLEAN" | "BOOL" => {
              let num: i16 = row.get(num);
              num.to_string()
            }
            "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
              let num: Vec<u8> = row.get(num);
              String::from_utf8_lossy(&num).to_string()
            }
            "CHAR" | "VARCHAR" => {
              let num: String = row.get(num);
              num
            }
            _ if vec_col_name[num] == cli.repcol => {
              let value: &str = row.get(num);
              value.replace("|", "").to_string()
            }
            _ => {
              let num: String = row.get(num);
              num
            }
          };
          vec_wtr_str.push(value);
        }
        wtr.serialize(vec_wtr_str)?;
        pb.inc(1);
      }
      wtr.flush()?;
      let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
      let output = format!("{}/{}.csv", &folder_path, cli.table);
      let output_log = format!("{} => {}\n", &timestamp, output);
      log_file.write_all(output_log.as_bytes())?;

      pb.finish_with_message("done");
    }
    Err(error) => {
      let err_msg = format!("Error with {}: {}", cli.table, error);
      error!("{}", &err_msg);
      let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
      let err_msg_log = format!("{} => {}\n", &timestamp, &err_msg);
      File::create(format!("{}/failed.log", cli.output)).expect("Failed to create file");
      let mut failed_file = OpenOptions::new()
        .append(true)
        .open(format!("{}/failed.log", cli.output))?;
      failed_file
        .write_all(err_msg.as_bytes())
        .expect("Failed to write to file");
      log_file.write_all(&err_msg_log.as_bytes())?;
    }
  }

  let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
  let msg_done_log = format!("{} => Done\n", &timestamp);
  log_file.write_all(msg_done_log.as_bytes())?;

  info!("All operations completed successfully.");

  Ok(())
}

fn folder_exists(path: &str) -> bool {
  std::fs::metadata(path).is_ok()
}

#[tokio::main]
async fn main() {
  let cli = Cli::parse();

  if std::env::var("RUST_LOG").is_err() {
    std::env::set_var("RUST_LOG", "info");
  }

  Builder::new()
    .format(|buf, record| {
      let level_color = match record.level() {
        Level::Error => Color::Red,
        Level::Warn => Color::Yellow,
        Level::Info => Color::Green,
        Level::Debug => Color::Blue,
        Level::Trace => Color::Purple,
      };

      writeln!(
        buf,
        "[{}] [{}] {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        level_color.paint(record.level().to_string()),
        record.args()
      )
    })
    .filter(None, LevelFilter::Info)
    .init();

  if let Err(err) = run(cli).await {
    error!("Application error: {}", err);
  }
}
