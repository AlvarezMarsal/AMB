using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;


namespace AmbHelper;

public class AmbDbConnection : DbConnection
{
    public readonly SqlConnection SqlConnection;
    private readonly Dictionary<string, string> _connectionStringParts;
    private LogFile? _log;
    private const string GetNextOidProc = "[dbo].[sp_internalGetNextOid]";

    public bool Log
    {
        get { return _log != null; }
        set
        {
            if (value)
            {
                _log ??= new LogFile("DatabaseLog");
            }
            else
            {
                _log?.Dispose();
                _log = null;
            }
            
        }
    }

    public AmbDbConnection(string connectionString, bool log = true)
    {
        if (log)
            _log = new LogFile("DatabaseLog");

        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        _connectionStringParts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var part in parts)
        {
            if (!string.IsNullOrEmpty(part))
            {
                var subparts = part.Split('=');
                if (subparts.Length == 2)
                    _connectionStringParts.Add(subparts[0].Trim(), subparts[1].Trim());
            }
        }

        SqlConnection = new SqlConnection(connectionString);
        SqlConnection.Open();

        /*
        var proc = $"""
                    CREATE OR ALTER PROCEDURE {GetNextOidProc}
                        (@oid BIGINT OUTPUT)
                    AS
                    SET NOCOUNT ON
                    BEGIN
                        SELECT @oid = [NextOID] FROM [t_AMBenchmarkSystem] WITH(TABLOCKX, HOLDLOCK);
                        UPDATE [t_AMBenchmarkSystem] SET [NextOID] = ([NextOID] + 1);
                    END
                    """;

        ExecuteNonQuery(proc);

        var db = _connectionStringParts["Database"];
        var grant = $"GRANT EXEC ON {db}.{GetNextOidProc} TO PUBLIC";
        ExecuteNonQuery(grant);
        */
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
        // ExecuteNonQuery($"DROP PROCEDURE ${GetNextOidProc}");
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
        try
        {
            var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            return cmd;
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
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
        _log?.WriteLine($"AmbDbConnection: executing non-query `{sql}`");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteNonQuery();
            _log?.WriteLine($"AmbDbConnection: nonquery command result {result}");
            return result;
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            return -1;
        }
    }

    public object? ExecuteScalar(string sql)
    {
        _log?.WriteLine($"AmbDbConnection: executing scalar `{sql}`");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteScalar();
            var r = result?.ToString() ?? "null";
            _log?.WriteLine($"AmbDbConnection: scalar command result {r}");
            return result;
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public SqlDataReader ExecuteReader(string sql)
    {
        _log?.WriteLine($"AmbDbConnection: executing reader `{sql}`");
        try
        {            
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            return cmd.ExecuteReader();
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
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
                    _log?.WriteLine($"AmbDbConnection: reader succeeded");
                    action(reader);
                }
                _log?.WriteLine($"AmbDbConnection: reader finished");
                _log?.Flush();
            }
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
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
        using var command = CreateCommand(GetNextOidProc);
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
        var result = command.ExecuteNonQuery();
        _log?.WriteLine($"AmbDbConnection: nonquery command result {result}");
        var oid = Convert.ToInt64(command.Parameters["@oid"].Value);
        _log?.WriteLine($"AmbDbConnection: new oid {oid}");
        return oid;
    }
}
