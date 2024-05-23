using System.Data;
using OfficeOpenXml;
using System.Data.SqlClient;
using System.Diagnostics;



namespace ImportLocations
{
    internal class Program
    {
        private readonly Settings _settings;
        private readonly DateTime _startTime = DateTime.Now;
        private readonly SqlConnection _connection;
        private readonly Guid _creationSession;
        private readonly Dictionary<long, GeographicLocation> _locations = new();
        private readonly SortedList<string, HashSet<long>> _aliases = [];
        private readonly string _collation = "COLLATE Latin1_General_100_BIN2";
        // ReSharper disable once CollectionNeverUpdated.Local
        private static readonly HashSet<string> Breakpoints;
        
        static Program()
        {
            Breakpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            Breakpoints.Add("Morton Grove");
        }

        //"D:\AMB\World Cities.xlsx" "Cities" "B1000713:H1048552" . AMBenchmark_DB 1
        static void Main(string[] args)
        {
            if (args.Length > 1)
            {
                Console.WriteLine("ImportLocations [settings.json]");
                return;
            }

            var arg0 = (args.Length > 0) ? args[0] : "importLocations.json";
            var settingsFileName = Path.GetFullPath(arg0);
            if (!File.Exists(settingsFileName))
            {
                Console.WriteLine($"Settings file not found: {settingsFileName}");
                return;
            }

            try
            {
                var settings = System.Text.Json.JsonSerializer.Deserialize<Settings>(File.ReadAllText(settingsFileName));
                var program = new Program(settings!);
                program.Run();

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private Program(Settings settings)
        {
            _settings = settings;
            _connection = new SqlConnection(_settings.ConnectionString);
            _creationSession = (_settings.CreationSession == null) ? Guid.NewGuid() : Guid.Parse(_settings.CreationSession);
        }

        private void Run()
        {
            using (_connection)
            {
                try
                {
                    _connection.Open();
                    CreateViews();
                    EnforcePresets();

                    foreach (var importFile in _settings.ImportFiles)
                    {
                        Run(importFile);
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
                finally
                {
                    Dump();
                    DeleteViews();
                }
            }
        }
        
        private void CreateViews()
        {
            var view = """
                        CREATE OR ALTER VIEW [dbo].[vw_GeographicLocationNames]
                        AS
                        	SELECT [OID], ISNULL([PID],0) AS PID, [Name], 1 AS IsPrimary
                        	FROM [dbo].[t_GeographicLocation]
                        
                        	UNION
                        
                        	SELECT L.[OID], ISNULL(L.[PID],0) AS PID, A.[Alias] AS [Name], 0 AS IsPrimary
                        	FROM [dbo].[t_GeographicLocationAlias] A 
                        	JOIN [dbo].[t_GeographicLocation] L ON L.OID = A.GeographicLocationID
                        	WHERE A.Alias <> L.Name COLLATE Latin1_General_100_BIN2
                        """;
            using var command = new SqlCommand(view, _connection);
            command.ExecuteNonQuery();
        }
        
        private void DeleteViews()
        {
            var drop = "DROP VIEW [dbo].[vw_GeographicLocationNames]";
            using var command = new SqlCommand(drop, _connection);
            command.ExecuteNonQuery();
        }
        
        private void EnforcePresets()
        {
            foreach (var preset in _settings.Presets)
            {
                try
                {
                    var pid = preset.Pid.GetValueOrDefault(0);
                    if (pid != 0)
                        LoadGeographicLocation(pid, true, 1);
                    
                    var n = preset.Name.Replace("'", "''");
                    var query = $"SELECT [OID], [PID] FROM [dbo].[vw_GeographicLocationNames] " +
                                $"WHERE [NAME] = N'{n}' {_collation} AND [PID] = {pid}";
                    if (preset.Oid != 0)
                        query += $" AND [OID] = {preset.Oid}";
                    
                    using (var command = new SqlCommand(query, _connection))
                    {
                        using var reader = command.ExecuteReader();
                        if (reader.Read())
                        {
                            var oid = reader.GetInt64(0);
                            if ((preset.Oid != 0) && (preset.Oid != oid))
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            var npid = reader.GetInt64(1);
                            if (npid != pid)
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            continue;
                        }
                    }
                    
                    AddGeographicLocation(0, pid, 2501, preset.Name, true);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    if (Debugger.IsAttached)
                        Debugger.Break();
                    throw;
                }
            }
        }

        private void Run(Settings.ImportFileInfo importInfo)
        {
            if (!File.Exists(importInfo.FilePath))
                throw new FileNotFoundException(importInfo.FilePath);

            Console.WriteLine($"Processing {importInfo.FilePath}");
            
            using var package = new ExcelPackage(new FileInfo(importInfo.FilePath));
            var isOneBased = package.Compatibility.IsWorksheets1Based;
            using var sheet = package.Workbook.Worksheets[importInfo.Sheet];

            var top = importInfo.FirstRow;
            var scanningTop = (top < 0);
            if (scanningTop)
                top = isOneBased ? 1 : 0;
            var bottom = importInfo.LastRow;
            var scanningBottom = (bottom < 0);
            if (scanningBottom)
                bottom = int.MaxValue;
            var dataStarted = !scanningTop;
            var dataEnded = false;

            var columnDefinitions = PreprocessColumnDefinitions(importInfo, isOneBased);
            
            // For each row in the spreadsheet, process each column
            for (var row=top; row<=bottom; ++row)
            {
                if (row % 100 == 0)
                    Console.WriteLine($"Processing row {row}");
                
                var skipRow = false;
                // reset, but keep any useful results from the last time through the loop
                foreach (var cd in columnDefinitions)
                {
                    cd.CurrentValue = sheet.Cells[row, cd.ColumnNumber].Text.Trim();
                    cd.AssignedGeographicLocation = null;
                }
                
                foreach (var cd in columnDefinitions)
                {
                    if (cd.Exclusions.Contains(cd.CurrentValue))
                    {
                        skipRow = true;
                        break;
                    }
                    
                    if (Debugger.IsAttached && Breakpoints.Contains(cd.CurrentValue))
                    {
                        Debug.WriteLine(cd.ToString());
                        Debugger.Break();
                    }

                    // If a non-optional cell is empty, that indicates the end of the data
                    // (if we don't have a predefined range).
                    if (string.IsNullOrEmpty(cd.CurrentValue) && !cd.SettingsDefinition.Optional)
                    {
                        if (scanningTop && !dataStarted)
                        {
                            skipRow = true;
                            break;
                        }

                        if (scanningBottom)
                        {
                            dataEnded = true;
                            Console.WriteLine($"Detected end of data at row {row}");
                            break;
                        }

                        throw new InvalidOperationException($"No value for cell {cd.ColumnNumber}:{row}");
                    }
                }

                if (skipRow)
                    continue;
                if (dataEnded)
                    break;
                if (!dataStarted)
                {
                    Console.WriteLine($"Detected start of data at row {row}");
                    dataStarted = true;
                }

                // process each in turn.  The way PreprocessColumnDefinitions works, we are guaranteed
                // that parents are processed before children.
                foreach (var cd in columnDefinitions)
                {
                    if (cd.AssignedGeographicLocation == null)
                        ProcessCell(cd);
                }

                // Now for aliases
                foreach (var cd in columnDefinitions)
                {
                    if (!string.IsNullOrEmpty(cd.CurrentValue) && (cd.AliasOf != null))
                    {
                        AddAliasIfNotPresent(cd.AliasOf.AssignedGeographicLocation!, cd.CurrentValue, false);
                    }
                }
            }
            
            return;

            void ProcessCell(ColumnDefinition coldef)
            {
                // Make sure that the columns it requires have been satisfied
                if (coldef.Parent is { AssignedGeographicLocation: null })
                {
                    ProcessCell(coldef.Parent);
                    if (coldef.Parent.AssignedGeographicLocation == null)
                        throw new InvalidOperationException("Parent cell is empty");
                }

                if (coldef.AliasOf is { AssignedGeographicLocation: null })
                {
                    ProcessCell(coldef.AliasOf);
                    if (coldef.AliasOf.AssignedGeographicLocation == null)
                        throw new InvalidOperationException("Predecessor cell is empty");
                }
                
                // If the cell is empty, it must be optional (otherwise we threw an exception above)
                // Since it might still be someone's parent, we use this cell's parent as its
                // assigned location.
                if (string.IsNullOrEmpty(coldef.CurrentValue))
                {
                    if (coldef.MustExist)
                        throw new InvalidOperationException("Empty cell");
                    if (coldef.AliasOf != null)
                        coldef.AssignedGeographicLocation = coldef.AliasOf.AssignedGeographicLocation;
                    else if (coldef.Parent != null)
                        coldef.AssignedGeographicLocation = coldef.Parent.AssignedGeographicLocation;
                }
                else if (coldef.MustExist)
                { 
                    coldef.AssignedGeographicLocation = 
                        LoadGeographicLocationByName(coldef.CurrentValue, null) ??
                        throw new InvalidOperationException($"Missing {coldef.CurrentValue}");
                }
                else if (coldef.Parent != null)
                {
                    var pal = coldef.Parent.AssignedGeographicLocation!;
                    coldef.AssignedGeographicLocation = 
                        LoadGeographicLocationByName(coldef.CurrentValue, pal.Oid) ??
                        AddGeographicLocation(0, pal.Oid, pal.PracticeAreaId, coldef.CurrentValue, coldef.IsSystemOwned);
                }

                if (coldef.AliasOf != null)
                {
                    var pal = coldef.AliasOf.AssignedGeographicLocation!;
                    coldef.AssignedGeographicLocation = pal;
                    AddAliasIfNotPresent(coldef.AssignedGeographicLocation, coldef.CurrentValue, false);
                }
            }
        }

        /// <summary>
        /// Retusn a list of ColumnDefinitions in the order they should be processed.
        /// </summary>
        /// <param name="importInfo"></param>
        /// <param name="spreadsheetIsOneBased"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        private List<ColumnDefinition> PreprocessColumnDefinitions(Settings.ImportFileInfo importInfo, bool spreadsheetIsOneBased)
        {
            // Parse our column definitions
            var columnDefinitions = new SortedList<string, ColumnDefinition>(); // columnName -> ColumnDefinition
            foreach (var icd in importInfo.ColumnDefinitions)
            {
                var cd = new ColumnDefinition(icd, spreadsheetIsOneBased);
                if (!columnDefinitions.TryAdd(cd.ColumnName, cd))
                    throw new InvalidOperationException($"Duplicate column {cd.ColumnName}");
            }

            // Rearrange the 'AliasOf' and 'ParentOf' data
            foreach (var cd in columnDefinitions.Values)
            {
                foreach (var childColumnName in cd.SettingsDefinition.ParentOf)
                {
                    if (!columnDefinitions.TryGetValue(childColumnName, out var child))
                        throw new InvalidOperationException($"Unknown parent {childColumnName}");
                    if (child.Parent != null)
                        throw new InvalidOperationException($"Duplicate parent {childColumnName}");
                    child.Parent = cd;
                }

                if (cd.SettingsDefinition.AliasOf != null)
                {
                    if (!columnDefinitions.TryGetValue(cd.SettingsDefinition.AliasOf, out var aliased))
                        throw new InvalidOperationException($"Unknown alias {cd.SettingsDefinition.AliasOf}");
                    cd.AliasOf = aliased;
                }
            }

            return columnDefinitions.Values.ToList();
        }

        internal static int ColumnAlphaToColumnNumber(string alpha, bool isOneBased)
        {
            var result = 0;
            foreach (var ch in alpha)
            {
                if (!char.IsLetter(ch))
                    break;
                if (char.IsLower(ch))
                    throw new ArgumentException("Invalid column name");
                result = result * 26 + (ch - 'A');
            }
            return result + (isOneBased ? 1 : 0);
        }

        private void LoadChildren(GeographicLocation parent)
        {
            // Connect to the database and load the world and continent records
            var children = Select<(long,string)>($"SELECT [OID], [Name] FROM [dbo].[vw_GeographicLocationNames] WHERE [PID] = {parent.Oid}", 
                reader => new (reader.GetInt64(0), reader.GetString(1)));
            
            foreach (var child in children)
            {
                if (_aliases.TryGetValue(child.Item2, out var oids))
                    oids.Add(child.Item1);
                else
                    _aliases.Add(child.Item2, [child.Item1]);
                
                var childLocation = LoadGeographicLocation(child.Item1, true);
                parent.Children.Add(child.Item2, childLocation);
            }

            parent.ChildrenLoaded = true;
        }

        /// <summary>
        /// Checks the cache before loading
        /// </summary>
        /// <param name="oid"></param>
        /// <param name="loadAliases"></param>
        /// <param name="recursionLevel"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        private GeographicLocation LoadGeographicLocation(long oid, bool loadAliases, int recursionLevel = 0)
        {
            if (!_locations.TryGetValue(oid, out var location))
            {
                using (var command = new SqlCommand($"SELECT [PID], [Name], [Index], [Description], [PracticeAreaID], [IsSystemOwned] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}", _connection))
                {
                    using var reader = command.ExecuteReader();
                    if (!reader.Read())
                        throw new InvalidOperationException($"No record found for {oid}");
                    
                    var columnIndex = 0;
                    var pid = reader.IsDBNull(columnIndex) ? (long?) null : reader.GetInt64(columnIndex);
                    var name = reader.GetString(++columnIndex);
                    var index = reader.GetInt32(++columnIndex);
                    var description = reader.GetString(++columnIndex);
                    var paid = reader.GetInt64(++columnIndex);
                    var isSystemOwned = reader.GetBoolean(++columnIndex);
                    
                    location = new GeographicLocation(oid, pid, name, index, description, paid, isSystemOwned);
                    _locations.Add(oid, location);
                    if (!_aliases.TryGetValue(location.Name, out var existingAliases))
                        _aliases.Add(location.Name, [oid]);
                    else
                        existingAliases.Add(oid);
                }
                
                var n = location.Name.Replace("'", "''");
                using (var command = new SqlCommand($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid} AND [Alias] <> N'{n}'", _connection))
                {
                    using var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var alias = reader.GetString(0);
                        if (!_aliases.TryGetValue(alias, out var existingAliases))
                            _aliases.Add(alias, [oid]);
                        else
                            existingAliases.Add(oid);
                    }
                }

                if (loadAliases)
                {
                    using var command = new SqlCommand($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid}", _connection);
                    using var reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        var alias = reader.GetString(0);
                        if (!_aliases.TryGetValue(alias, out var existingAliases))
                            _aliases.Add(alias, [oid]);
                        else
                            existingAliases.Add(oid);
                    }
                }
            }

            if (recursionLevel-- > 0)
            {
                if (!location.ChildrenLoaded)
                    LoadChildren(location);
                foreach (var child in location.Children.Values)
                {
                    LoadGeographicLocation(child.Oid, loadAliases, recursionLevel);
                }
            }
            
            return location;
        }

        
        private GeographicLocation? LoadGeographicLocationByName(string name, long? pid)
        {
            if ((pid != null) && _aliases.TryGetValue(name, out var oids))
            {
                foreach (var o in oids)
                {
                    var location = LoadGeographicLocation(o, false);
                    if (location.Pid == pid)
                        return location;
                }
            }

            var n = name.Replace("'", "''");
            var query = $"SELECT V.[OID] FROM [dbo].[vw_GeographicLocationNames] V " +
                        $"WHERE V.[Name] = N'{n}' {_collation}";
            if (pid != null)
                query += $" AND V.[PID] = {pid}";

            using var command = new SqlCommand(query, _connection);
            long oid;
            using (var reader = command.ExecuteReader())
            {
                if (!reader.Read())
                    return null;
                oid = reader.GetInt64(0);
                while (reader.Read())
                {
                    var otherOid = reader.GetInt64(0);
                    if (otherOid != oid)
                        throw new InvalidOperationException($"Conflicting records found for {name}");
                }
            }
            return LoadGeographicLocation(oid, true);
        }

        private GeographicLocation AddGeographicLocation(long oid, long pid, long? practiceAreaId, string name, bool isSystemOwned)
        {
            if (oid < 1)
                oid = GetNextOid();
            var parent = (pid == 0) ? null : LoadGeographicLocation(pid, true, 1); // includes children
            int index;
            if (parent == null)
            {
                index = 0;
                practiceAreaId ??= 2501;
            }
            else
            {
                if (!parent.ChildrenLoaded)
                    LoadChildren(parent);
                index = (parent.Children.Count == 0) ? 0 : parent.Children.Values.Max(entity => entity.Index) + 1;
                practiceAreaId ??= parent.PracticeAreaId;
            }

            var description = name;

            try
            {
                var pidstr = (pid == 0) ? "NULL" : pid.ToString();
                var n = name.Replace("'", "''");
                var d = description.Replace("'", "''");
                var iso = isSystemOwned ? 1 : 0;
                using var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocation] ([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorID],[PracticeAreaID],[CreationSession]) " +
                                                   $"VALUES({oid}, {pidstr}, {iso}, N'{n}', {index}, N'{d}', '{_startTime}', {_settings.CreatorId}, {practiceAreaId}, '{_creationSession}')", _connection);
                var result = command.ExecuteNonQuery();
                if (result != 1)
                    throw new InvalidOperationException($"Insert failed for {name}");
            }
            catch (Exception e)
            {
                Debug.WriteLine(e);
                Console.WriteLine(e);
                if (Debugger.IsAttached)
                    Debugger.Break();
                throw;
            }

            var location = new GeographicLocation(oid, pid, name, index, description, practiceAreaId.Value, isSystemOwned);
            _locations.Add(oid, location);
            parent?.Children.Add(name, location);

            AddAliasIfNotPresent(location, name, true);
            return location;
        }
        
        private void AddAliasIfNotPresent(GeographicLocation location, string alias, bool isPrimary)
        {
            if (_aliases.TryGetValue(alias, out var existing) && existing.Contains(location.Oid))
                return;

            var a = alias.Replace("'", "''");
            try
            {
                using var command = new SqlCommand($"SELECT COUNT(*) FROM [dbo].[vw_GeographicLocationNames] " +
                                                   $"WHERE [Name] = N'{a}' {_collation} AND [OID] = {location.Oid}",
                                                   _connection);
                var result = command.ExecuteScalar();
                if ((result is long and > 0))
                    return;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
             
            var aliasOid = GetNextOid();
            var description = alias;
            var d = description.Replace("'", "''");
            var practiceAreaId = location.PracticeAreaId;
            var isPrimaryInt = isPrimary ? 1 : 0;
            var isSystemOwnedInt = location.IsSystemOwned ? 1 : 0;
            using (var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocationAlias] ([OID],[GeographicLocationID],[Alias],[Description],[IsPrimary],[PracticeAreaID],[CreationDate],[CreatorID],[CreationSession],[LID],[IsSystemOwned]) " +
                                               $"VALUES({aliasOid}, {location.Oid}, N'{a}', N'{d}', {isPrimaryInt}, {practiceAreaId}, '{_startTime}', {_settings.CreatorId}, '{_creationSession}', 500, {isSystemOwnedInt})", _connection))
            { 
                var result = command.ExecuteNonQuery();
                if (result != 1)
                    throw new InvalidOperationException($"Insert failed for {alias}");
            }

            if (_aliases.TryGetValue(alias, out existing))
                existing.Add(location.Oid);
            else
                _aliases.Add(alias, [location.Oid]);

        }
        

        private long GetNextOid()
        {
            using var command = new SqlCommand("[dbo].sp_internalGetNextOID", _connection);
            command.CommandType = CommandType.StoredProcedure; 
            command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
            command.ExecuteNonQuery();
            return Convert.ToInt64(command.Parameters["@oid"].Value);
        }

        private List<T> Select<T>(string query, Func<IDataReader, T> build)
        {
            var list = new List<T>();
            using var command = new SqlCommand(query, _connection);
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var t = build(reader);
                list.Add(t);
            }
            return list;
        }

        private void Dump()
        {
            const string filename = "dump.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using var file = File.CreateText(filename);
            DumpLocationsUnder(0, 0, file);
        }

        private void DumpLocationsUnder(long pid, int indent, StreamWriter file)
        {
            var children = Select($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE ISNULL([PID],0) = {pid} ORDER BY [Name]",
                reader => reader.GetInt64(0));
            
            foreach (var child in children)
            {
                for (var i=0; i < indent; ++i)
                    file.Write("\t");
                var names = Select($"SELECT [Name] FROM [dbo].[vw_GeographicLocationNames] WHERE [OID] = {child} ORDER BY IsPrimary DESC, Name ASC",
                    reader => reader.GetString(0));
                var n = string.Join(", ", names);
                //if (Debugger.IsAttached)
                //    Debug.WriteLine($"{child} {n}");
                file.WriteLine($"{child} {n}");
                DumpLocationsUnder(child, indent+1, file);
            }
        }
    }
}
