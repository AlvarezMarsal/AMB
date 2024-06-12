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
        => CreateCommand(sql, true);

    private SqlCommand CreateCommand(string sql, bool log)
    {
        try
        {
            var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            if (log)
                _log?.WriteLine($"AmbDbConnection: created command \"{sql}\"");
            return cmd;
        }
        catch (Exception e)
        {
            if (log)
            {
                _log?.WriteLine($"AmbDbConnection: Exception while creating command \"{sql}\"");
                _log?.WriteLine(e.ToString());
                _log?.Flush();
                if (Debugger.IsAttached)
                    Debugger.Break();
            }
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
        //_log?.WriteLine($"AmbDbConnection: executing non-query \"{sql}\"");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteNonQuery();
            _log?.WriteLine($"AmbDbConnection: {result} rows affected by non-query \"{sql}\"");
            return result;
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception thrown by non-query \"{sql}\"");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            return -1;
        }
    }

    public object? ExecuteScalar(string sql)
    {
        //_log?.WriteLine($"AmbDbConnection: executing scalar \"{sql}\"");
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteScalar();
            var r = result?.ToString() ?? "null";
            _log?.WriteLine($"AmbDbConnection: value {r} returned by scalar command \"{sql}\"");
            return result;
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception thrown by scalar command \"{sql}\"");
            _log?.WriteLine(e.ToString());
            _log?.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public SqlDataReader ExecuteReader(string sql)
        => ExecuteReader(sql, true);

    private SqlDataReader ExecuteReader(string sql, bool log)
    {
        try
        {
            using var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            if (log)
                _log?.WriteLine($"AmbDbConnection: executing reader \"{sql}\"");
            return cmd.ExecuteReader();
        }
        catch (Exception e)
        {
            if (log)
            {
                _log?.WriteLine($"AmbDbConnection: Exception thrown by reader \"{sql}\"");
                _log?.WriteLine(e.ToString());
                _log?.Flush();
                if (Debugger.IsAttached)
                    Debugger.Break();
            }
            throw;
        }
    }


    public void ExecuteReader(string sql, Action<SqlDataReader> action)
    {
        try
        {           
            using (var reader = ExecuteReader(sql, false))
            {
                int count = 0;
                while (reader.Read())
                {
                    ++count;
                    action(reader);
                }
                _log?.WriteLine($"AmbDbConnection: {count} reads from reader \"{sql}\"");
                // _log?.Flush();
            }
        }
        catch (Exception e)
        {
            _log?.WriteLine($"AmbDbConnection: Exception thrown by reader \"{sql}\"");
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

    

    public long GetNextOid()
    {
        using var command = CreateCommand(GetNextOidProc, false);
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
        var result = command.ExecuteNonQuery();
        _log?.WriteLine($"AmbDbConnection: nonquery command result {result}");
        var oid = Convert.ToInt64(command.Parameters["@oid"].Value);
        _log?.WriteLine($"AmbDbConnection: new oid {oid}");
        return oid;
    }
}
