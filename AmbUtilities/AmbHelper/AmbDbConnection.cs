using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;

namespace AmbHelper
{
    public class AmbDbConnection : DbConnection
    {
        public readonly SqlConnection SqlConnection;
        private readonly StreamWriter? _log;

        public AmbDbConnection(string connectionString, bool log = true)
        {
            _log = log ? File.CreateText("DatabaseLog.txt") : null;
            _log?.WriteLine($"AmbDbConnection: opening database `{connectionString}`");
            
            SqlConnection = new SqlConnection(connectionString);
            SqlConnection.Open();
        }
        
        // For outside callers to use
        public void Log(string message)
        {
            _log?.WriteLine(message);
        }

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        public override string ConnectionString { get => SqlConnection.ConnectionString; set => throw new InvalidOperationException(); }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

        public override string Database => SqlConnection.Database;

        public override string DataSource => SqlConnection.DataSource!;

        public override string ServerVersion => SqlConnection.ServerVersion!;

        public override ConnectionState State => SqlConnection.State;

        public override void ChangeDatabase(string databaseName)
        {
            SqlConnection.ChangeDatabase(databaseName);
        }

        public override void Close()
        {
            SqlConnection.Close();
        }

        public override void Open()
        {
            SqlConnection.Open();
        }

        protected override SqlTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            throw new InvalidOperationException();
        }

        protected override SqlCommand CreateDbCommand()
        {
            return SqlConnection.CreateCommand();
        }

        public SqlCommand CreateCommand(string sql)
        {
            _log?.WriteLine($"AmbDbConnection: creating command `{sql}`");
            var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            return cmd;
        }

        protected override void Dispose(bool disposing)
        {
            SqlConnection.Dispose();
            base.Dispose(disposing);
        }

        public int ExecuteNonQuery(string sql)
        {
            using var command = CreateCommand(sql);
            var result = command.ExecuteNonQuery();
            _log?.WriteLine($"AmbDbConnection: nonquery command result {result}");
            return result;
        }

        public object? ExecuteScalar(string sql)
        {
            try
            {
                using var command = CreateCommand(sql);
                var result = command.ExecuteScalar();
                if (_log != null)
                {
                    var r = result?.ToString() ?? "null";
                    _log.WriteLine($"AmbDbConnection: scalar command result {r}");
                }
                return result;
            }
            catch (Exception e)
            {
                _log?.WriteLine($"AmbDbConnection: Exception");
                _log?.WriteLine(e.ToString());
                if (Debugger.IsAttached)
                    Debugger.Break();
                throw;
            }
        }

        public SqlDataReader ExecuteReader(string sql)
        {
            try
            {            
                var command = CreateCommand(sql);
                _log?.WriteLine("AmbDbConnection: executing read");
                return command.ExecuteReader();
            }
            catch (Exception e)
            {
                _log?.WriteLine($"AmbDbConnection: Exception");
                _log?.WriteLine(e.ToString());
                if (Debugger.IsAttached)
                    Debugger.Break();
                throw;
            }
        }

        public void ExecuteReader(string sql, Action<SqlDataReader> action)
        {
            try
            {           
                using (var command = new SqlCommand(sql, SqlConnection))
                {
                    using var reader = command.ExecuteReader();
                    while (reader.Read())
                    {   
                        action(reader);
                    }
                }
            }
            catch (Exception e)
            {
                _log?.WriteLine($"AmbDbConnection: Exception");
                _log?.WriteLine(e.ToString());
                if (Debugger.IsAttached)
                    Debugger.Break();
                throw;
            }
        }


        public IEnumerable<T> ExecuteReader<T>(string sql, Func<SqlDataReader, T> func)
        {
            using (var command = CreateCommand(sql))
            {
                SqlDataReader reader;
                try                 
                {
                    reader = command.ExecuteReader();
                }
                catch (Exception e)
                {
                    _log?.WriteLine($"AmbDbConnection: Exception");
                    _log?.WriteLine(e.ToString());
                    throw;
                }
                
                while (reader.Read())
                {
                    yield return func(reader);
                }
                
                reader.Dispose();
            }
        }
        
        
        public List<T> Select<T>(string query, Func<IDataReader, T> build)
        {
            try                 
            {
                var list = new List<T>();
                using var command = CreateCommand(query);
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    var t = build(reader);
                    list.Add(t);
                }
                return list;
            }
            catch (Exception e)
            {
                _log?.WriteLine($"AmbDbConnection: Exception");
                _log?.WriteLine(e.ToString());
                throw;
            }
        }


        /*
        public bool ExecuteReaderOnce<T>(string sql, Action<SqlDataReader> action)
        {
            using (var command = new SqlCommand(sql, SqlConnection))
            {
                using var reader = command.ExecuteReader();
                if (!reader.Read())
                    return false;
                action(reader);
                return true;
            }
        }

        public T? ExecuteReaderOnce<T>(string sql, Func<SqlDataReader, T> func)
        {
            using (var command = new SqlCommand(sql, SqlConnection))
            {
                using var reader = command.ExecuteReader();
                if (!reader.Read())
                    return default;
                var t = func(reader);
                return t;
            }
        }

        
        public List<T> ExecuteList<T>(string sql, Func<SqlDataReader, T> func)
        {
            var list = new List<T>();
            ExecuteReader(sql, r => list.Add(func(r)));
            return list;
        }
        */
        

        public long GetNextOid()
        {
            using var command = CreateCommand("[dbo].sp_internalGetNextOID");
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
            var result = command.ExecuteNonQuery();
            _log?.WriteLine($"AmbDbConnection: nonquery command result {result}");
            return Convert.ToInt64(command.Parameters["@oid"].Value);
        }
    }
}
