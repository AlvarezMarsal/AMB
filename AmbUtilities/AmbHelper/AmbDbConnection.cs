using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static AmbHelper.Settings;

#nullable enable

namespace AmbHelper
{
    public class AmbDbConnection : DbConnection
    {
        public readonly SqlConnection SqlConnection;

        public AmbDbConnection(string connectionString)
        {
            SqlConnection = new SqlConnection(connectionString);
            SqlConnection.Open();
        }

#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        public override string ConnectionString { get => SqlConnection.ConnectionString; set => throw new NotImplementedException(); }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).

        public override string Database => SqlConnection.Database;

        public override string DataSource => SqlConnection.DataSource;

        public override string ServerVersion => SqlConnection.ServerVersion;

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
            throw new NotImplementedException();
        }

        protected override SqlCommand CreateDbCommand()
        {
            return SqlConnection.CreateCommand();
        }

        public SqlCommand CreateCommand(string sql)
        {
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
            using var command = new SqlCommand(sql, SqlConnection);
            return command.ExecuteNonQuery();
        }

        public object ExecuteScalar(string sql)
        {
            using var command = new SqlCommand(sql, SqlConnection);
            return command.ExecuteScalar();
        }

        public SqlDataReader ExecuteReader(string sql)
        {
            var command = new SqlCommand(sql, SqlConnection);
            return command.ExecuteReader();
        }

        public void ExecuteReader(string sql, Action<SqlDataReader> action)
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

        public IEnumerable<T> ExecuteReader<T>(string sql, Func<SqlDataReader, T> func)
        {
            using (var command = new SqlCommand(sql, SqlConnection))
            {
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    yield return func(reader);
                }
            }
        }

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

        public long GetNextOid()
        {
            using var command = CreateCommand("[dbo].sp_internalGetNextOID");
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
            command.ExecuteNonQuery();
            return Convert.ToInt64(command.Parameters["@oid"].Value);
        }
    }
}
