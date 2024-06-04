using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;

namespace AmbHelper;

public class AmbDbConnection : DbConnection
{
    public readonly SqlConnection SqlConnection;
    private readonly LogFile Log;

    public AmbDbConnection(string connectionString, bool log = true)
    {
        Log = new LogFile("DatabaseLog");            
        SqlConnection = new SqlConnection(connectionString);
        SqlConnection.Open();
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
        Log.WriteLine($"AmbDbConnection: creating command `{sql}`");
        try
        {
            var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            return cmd;
        }
        catch (Exception e)
        {
            Log.WriteLine($"AmbDbConnection: Exception");
            Log.WriteLine(e.ToString());
            Log.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    protected override void Dispose(bool disposing)
    {
        SqlConnection.Dispose();
        base.Dispose(disposing);
    }

    public int ExecuteNonQuery(string sql)
    {
        Log.WriteLine($"AmbDbConnection: executing non-query `{sql}`");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteNonQuery();
            Log.WriteLine($"AmbDbConnection: nonquery command result {result}");
            return result;
        }
        catch (Exception e)
        {
            Log.WriteLine($"AmbDbConnection: Exception");
            Log.WriteLine(e.ToString());
            Log.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            return -1;
        }
    }

    public object? ExecuteScalar(string sql)
    {
        Log.WriteLine($"AmbDbConnection: executing scalar `{sql}`");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteScalar();
            var r = result?.ToString() ?? "null";
            Log.WriteLine($"AmbDbConnection: scalar command result {r}");
            return result;
        }
        catch (Exception e)
        {
            Log.WriteLine($"AmbDbConnection: Exception");
            Log.WriteLine(e.ToString());
            Log.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public SqlDataReader ExecuteReader(string sql)
    {
        Log.WriteLine($"AmbDbConnection: executing reader `{sql}`");
        try
        {            
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteReader();
        }
        catch (Exception e)
        {
            Log.WriteLine($"AmbDbConnection: Exception");
            Log.WriteLine(e.ToString());
            Log.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public void ExecuteReader(string sql, Action<SqlDataReader> action)
    {
        try
        {           
            using (var reader = ExecuteReader(sql))
            {
                while (reader.Read())
                {   
                    Log.WriteLine($"AmbDbConnection: reader succeeded");
                    action(reader);
                }
                Log.WriteLine($"AmbDbConnection: reader finished");
                Log.Flush();
            }
        }
        catch (Exception e)
        {
            Log.WriteLine($"AmbDbConnection: Exception");
            Log.WriteLine(e.ToString());
            Log.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }


    public IEnumerable<T> ExecuteReader<T>(string sql, Func<SqlDataReader, T> func)
    {
        var list = new List<T>();
        ExecuteReader(sql, r => list.Add(func(r)));
        return list;
    }
    
    
    public List<T> Select<T>(string query, Func<IDataReader, T> build)
    {
        var list = new List<T>();
        ExecuteReader(query, r => list.Add(build(r)));
        return list;

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
        Log.WriteLine($"AmbDbConnection: nonquery command result {result}");
        var oid = Convert.ToInt64(command.Parameters["@oid"].Value);
        Log.WriteLine($"AmbDbConnection: new oid {oid}");
        return oid;
    }
}
