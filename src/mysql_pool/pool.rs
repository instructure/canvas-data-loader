use mysql::error::Error as MysqlError;
use mysql::conn::Conn as MysqlBaseConn;
use mysql::Opts as MysqlOpts;
use mysql::OptsBuilder as MysqlOptsBuilder;
use r2d2::ManageConnection as R2D2ManageConnection;

#[derive(Clone, Debug)]
pub struct MysqlConnectionManager {
  params: MysqlOpts,
}

pub trait CreateManager<T> {
  type Manager;

  fn new(params: T) -> Result<Self::Manager, MysqlError>;
}

impl CreateManager<MysqlOptsBuilder> for MysqlConnectionManager {
  type Manager = MysqlConnectionManager;

  fn new(params: MysqlOptsBuilder) -> Result<Self::Manager, MysqlError> {
    Ok(MysqlConnectionManager {
      params: MysqlOpts::from(params),
    })
  }
}

impl <'a> CreateManager<&'a str> for MysqlConnectionManager {
  type Manager = MysqlConnectionManager;

  fn new(params: &'a str) -> Result<Self::Manager, MysqlError> {
    Ok(MysqlConnectionManager {
      params: MysqlOpts::from(params),
    })
  }
}

impl R2D2ManageConnection for MysqlConnectionManager {
  type Connection = MysqlBaseConn;
  type Error = MysqlError;

  fn connect(&self) -> Result<MysqlBaseConn, MysqlError> {
    MysqlBaseConn::new(self.params.clone())
  }

  fn is_valid(&self, conn: &mut MysqlBaseConn) -> Result<(), MysqlError> {
    conn.query("SELECT 1;").map(|_| ())
  }

  fn has_broken(&self, conn: &mut MysqlBaseConn) -> bool {
    self.is_valid(conn).is_err()
  }
}
