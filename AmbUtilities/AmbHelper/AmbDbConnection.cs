using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using static AmbHelper.Logs;

namespace AmbHelper;

public class AmbDbConnection : DbConnection
{
    public readonly SqlConnection SqlConnection;
    private readonly Dictionary<string, string> _connectionStringParts;
    private const string GetNextOidProc = "[dbo].[sp_internalGetNextOid]";
    //private const string GetNextOidsProc = "[dbo].[sp_internalGetNextOids]";
    private bool _currentLoggingSettting = true;
    public bool CurrentLoggingSetting
    {
        get => _currentLoggingSettting;
        set { _currentLoggingSettting = value; }
    }

    public AmbDbConnection(string connectionString)
    {
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
    }

    [AllowNull]
    public override string ConnectionString { get => SqlConnection.ConnectionString; set => throw new InvalidOperationException(); }

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
        => CreateCommand(sql, _currentLoggingSettting);

    public SqlCommand CreateCommand(string sql, bool log)
    {
        try
        {
            var cmd = SqlConnection.CreateCommand();
            cmd.CommandText = sql;
            if (log)
                Log.WriteLine($"AmbDbConnection: created command \"{sql}\"");
            return cmd;
        }
        catch (Exception e)
        {
            if (log)
            {
                Error.WriteLine($"AmbDbConnection: Exception while creating command \"{sql}\"", e);
                Error.Flush();
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
        => ExecuteNonQuery(sql, _currentLoggingSettting);

    public int ExecuteNonQuery(string sql, bool log)
    {
        //_log?.WriteLine($"AmbDbConnection: executing non-query \"{sql}\"");
        try
        {
            using var cmd = CreateCommand(sql, log);
            return ExecuteNonQuery(cmd, log);
        }
        catch
        {
            return -1;
        }
    }

    public int ExecuteNonQuery(SqlCommand cmd)
        => ExecuteNonQuery(cmd, _currentLoggingSettting);

    public int ExecuteNonQuery(SqlCommand cmd, bool log)
    {
        //_log?.WriteLine($"AmbDbConnection: executing non-query \"{sql}\"");
        try
        {
            var result = cmd.ExecuteNonQuery();
            if (log)
                Log.WriteLine($"AmbDbConnection: {result} rows affected by non-query \"{cmd.CommandText}\"");
            return result;
        }
        catch (Exception e)
        {
            Error.WriteLine($"AmbDbConnection: Exception thrown by non-query \"{cmd.CommandText}\"", e);
            Error.Flush();
            if (Debugger.IsAttached)
               Debugger.Break();
            return -1;
        }
    }

    public object? ExecuteScalar(string sql)
        => ExecuteScalar(sql, _currentLoggingSettting);

    public object? ExecuteScalar(string sql, bool log)
    {
        //_log?.WriteLine($"AmbDbConnection: executing scalar \"{sql}\"");
        try
        {
            using var cmd = CreateCommand(sql, log);
            var result = cmd.ExecuteScalar();
            var r = result?.ToString() ?? "null";
            Log.WriteLine($"AmbDbConnection: value {r} returned by scalar command \"{sql}\"");
            return result;
        }
        catch (Exception e)
        {
            Error.WriteLine($"AmbDbConnection: Exception thrown by scalar command \"{sql}\"");
            Error.WriteLine(e.ToString());
            Error.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public SqlDataReader ExecuteReader(string sql)
        => ExecuteReader(sql, _currentLoggingSettting);

    public SqlDataReader ExecuteReader(string sql, bool log)
    {
        try
        {
            using var cmd = CreateCommand(sql, log);
            cmd.CommandText = sql;
            if (log)
                Log.WriteLine($"AmbDbConnection: executing reader \"{sql}\"");
            return cmd.ExecuteReader();
        }
        catch (Exception e)
        {
            if (log)
            {
                Error.WriteLine($"AmbDbConnection: Exception thrown by reader \"{sql}\"");
                Error.WriteLine(e.ToString());
                Error.Flush();
                if (Debugger.IsAttached)
                    Debugger.Break();
            }
            throw;
        }
    }

    public void ExecuteReader(string sql, Action<SqlDataReader> action)
        => ExecuteReader(sql, action, _currentLoggingSettting);

    public void ExecuteReader(string sql, Action<SqlDataReader> action, bool log)
    {
        try
        {           
            using (var reader = ExecuteReader(sql, log))
            {
                int count = 0;
                while (reader.Read())
                {
                    ++count;
                    action(reader);
                }
                if (log)
                {
                    if (count == 1)
                        Log.WriteLine($"AmbDbConnection: 1 read from reader \"{sql}\"");
                    else
                        Log.WriteLine($"AmbDbConnection: {count} reads from reader \"{sql}\"");
                }
                // _log?.Flush();
            }
        }
        catch (Exception e)
        {
            Error.WriteLine($"AmbDbConnection: Exception thrown by reader \"{sql}\"");
            Error.WriteLine(e.ToString());
            Error.Flush();
            if (Debugger.IsAttached)
                Debugger.Break();
            throw;
        }
    }

    public IEnumerable<T> ExecuteReader<T>(string sql, Func<SqlDataReader, T> func)
        => ExecuteReader(sql, func, _currentLoggingSettting);

    public IEnumerable<T> ExecuteReader<T>(string sql, Func<SqlDataReader, T> func, bool log)
    {
        var list = new List<T>();
        ExecuteReader(sql, r => list.Add(func(r)), log);
        return list;
    }
    
    public List<T> Select<T>(string query, Func<IDataReader, T> build)
        => Select(query, build, _currentLoggingSettting);

    public List<T> Select<T>(string query, Func<IDataReader, T> build, bool log)
    {
        var list = new List<T>();
        ExecuteReader(query, r => list.Add(build(r)), log);
        return list;
    }

    public Dictionary<TKey, TValue> Select<TKey, TValue>(string query, Func<IDataReader, KeyValuePair<TKey, TValue>> build)
            where TKey : notnull
        => Select(query, build, _currentLoggingSettting);

    public Dictionary<TKey, TValue> Select<TKey, TValue>(string query, Func<IDataReader, KeyValuePair<TKey, TValue>> build, bool log)
        where TKey : notnull
    {
        var dictionary = new Dictionary<TKey, TValue>();
        ExecuteReader(query, r => 
        {
            var kvp = build(r);
            dictionary.Add(kvp.Key, kvp.Value);

        }, log);
        return dictionary;
    }

    public Dictionary<TKey, TValue> Select<TKey, TValue>(string query, Func<IDataReader, Tuple<TKey, TValue>> build) 
            where TKey : notnull
        => Select(query, build, _currentLoggingSettting);

    public Dictionary<TKey, TValue> Select<TKey, TValue>(string query, Func<IDataReader, Tuple<TKey, TValue>> build, bool log) where TKey : notnull
    {
        var d = new Dictionary<TKey, TValue>();
        ExecuteReader(query, r =>
        {
            var tuple = build(r);
            d.Add(tuple.Item1, tuple.Item2);
        }, log);
        return d;
    }

    public T? SelectOneValue<T>(string query, Func<IDataReader, T> build) 
            where T : struct
        => SelectOneValue(query, build, _currentLoggingSettting);


    public T? SelectOneValue<T>(string query, Func<IDataReader, T> build, bool log) where T : struct
    {
        T? result = default;
        ExecuteReader(query, r => result = build(r), log);
        return result;
    }   

    public T? SelectOne<T>(string query, Func<IDataReader, T> build) where T : class
        => SelectOne(query, build, _currentLoggingSettting);  

    public T? SelectOne<T>(string query, Func<IDataReader, T> build, bool log) where T : class
    {
        T? result = default;
        ExecuteReader(query, r => result = build(r), log);
        return result;
    }   

    public long GetNextOid()
        => GetNextOid(_currentLoggingSettting);

    public long GetNextOid(bool log)
    {
        using var command = CreateCommand(GetNextOidProc, false);
        command.CommandType = CommandType.StoredProcedure;
        command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
        var result = command.ExecuteNonQuery();
        if (log)
            Log.WriteLine($"AmbDbConnection: nonquery command result {result}");
        var oid = Convert.ToInt64(command.Parameters["@oid"].Value);
        if (log)
            Log.WriteLine($"AmbDbConnection: new oid {oid}");
        return oid;
    }

    /*
    private const int CachedOidCount = 100; 
    private static readonly Queue<long> _cachedOids = new Queue<long>(CachedOidCount);

    public long GetNextOidCached(bool log = false)
    {
        if (_cachedOids.Count == 0)
        {
            using var command = CreateCommand(GetNextOidsProc, log);
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add("@count", SqlDbType.Int, 4).Value = CachedOidCount;
            using var reader = command.ExecuteReader();
            while (reader.Read())
                _cachedOids.Enqueue(reader.GetInt64(1));
        }

        return _cachedOids.Dequeue();
    }
    */
}
