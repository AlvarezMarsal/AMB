using OfficeOpenXml;
using System.Diagnostics;
using AmbHelper;
using System.Data;

namespace ImportLocations
{
    internal class Program
    {
        private readonly Settings _settings;
        private readonly DateTime _startTime = DateTime.Now;
        private readonly AmbDbConnection _connection;
        private readonly Guid _creationSession;
        private readonly HashSet<string> _aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<long, GeographicLocation> _locationCacheByOid = new();
        private readonly Dictionary<string, GeographicLocation> _locationCacheByName = new Dictionary<string, GeographicLocation>(StringComparer.OrdinalIgnoreCase);
        //private readonly SortedList<string, HashSet<long>> _aliases = [];
        private string NameCollation => "COLLATE " + _settings.NameCollation;
        private string AliasCollation => "COLLATE " + _settings.AliasCollation;
        // ReSharper disable once CollectionNeverUpdated.Local
        private static readonly HashSet<string> Breakpoints;
        
        static Program()
        {
            Breakpoints = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            // Breakpoints.Add("Morton Grove");
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
                Log.WriteLine(e);
                throw;
            }
        }

        private Program(Settings settings)
        {
            _settings = settings;
            _connection = new AmbDbConnection(_settings.ConnectionString);
            _creationSession = Guid.Parse(_settings.CreationSession);
        }

        private void Run()
        {
            using (_connection)
            {

                try
                {
                    //Dump();
                    CreateViews();
                    EnforcePresets();

                    foreach (var importFile in _settings.Imports)
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
            /*
            var view = $"""
                        CREATE OR ALTER VIEW [dbo].[vw_GeographicLocationNames]
                        AS
                        	SELECT [OID], ISNULL([PID],0) AS PID, cast([Name] {AliasCollation} as nvarchar(512)) AS [Name], 1 AS IsPrimary, [Index]
                        	FROM [dbo].[t_GeographicLocation]
                        
                        	UNION
                        
                        	SELECT L.[OID], ISNULL(L.[PID],0) AS PID, cast(A.[Alias] {AliasCollation} as nvarchar(512)) AS [Name], 0 AS IsPrimary, [Index]
                        	FROM [dbo].[t_GeographicLocationAlias] A 
                        	JOIN [dbo].[t_GeographicLocation] L ON L.OID = A.GeographicLocationID
                        	WHERE A.Alias <> L.Name {AliasCollation}
                        """;
            _connection.ExecuteNonQuery(view);
            */
            /*
            try
            {
                var disableIndex = "ALTER TABLE [dbo].[t_GeographicLocationAlias] DROP CONSTRAINT [UQ__t_Geogra__4C49A2004F2CE66A]";
                _connection.ExecuteNonQuery(disableIndex);
            }
            catch (Exception e)
            {
                Log.WriteLine(e);
            }
            
            try
            {
                var changeCollation = $"ALTER TABLE [dbo].[t_GeographicLocationAlias] ALTER COLUMN [Alias] NVARCHAR(512) {AliasCollation}";
                _connection.ExecuteNonQuery(changeCollation);
                
                var createIndex = """
                                    ALTER TABLE [dbo].[t_GeographicLocationAlias] ADD  CONSTRAINT [UQ__t_Geogra__4C49A2004F2CE66A] UNIQUE NONCLUSTERED
                                    ([GeographicLocationID] ASC, [Alias] ASC)
                                    WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                                    """;
                _connection.ExecuteNonQuery(createIndex);            
            }
            catch (Exception e)
            {
                Log.WriteLine(e);
            }
            */
        }
        
        private void DeleteViews()
        {
            var drop = "DROP VIEW [dbo].[vw_GeographicLocationNames]";
            _connection.ExecuteNonQuery(drop);
        }
        
        private void EnforcePresets()
        {
            foreach (var preset in _settings.Presets)
            {
                try
                {
                    var pid = preset.PID.HasValue ? $"[PID] = {preset.PID}" : "[PID] IS NULL";
                    //if (pid != 0)
                    //    LoadGeographicLocation(pid, true, 1);

                    var n = preset.Name.Replace("'", "''").ToLower();
                    var query = $"SELECT [OID], COALESCE([PID],0), [Index] FROM [dbo].[t_GeographicLocation] " +
                                $"WHERE LOWER([NAME]) = N'{n}' {NameCollation} AND {pid}";
                    if (preset.OID != 0)
                        query += $" AND [OID] = {preset.OID}";

                    var locations = _connection.Select(query,
                        (reader) =>
                        {
                            var oid = reader.GetInt64(0);
                            if ((preset.OID != 0) && (preset.OID != oid))
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            var pid = reader.GetInt64(1);
                            var p = preset.PID.GetValueOrDefault(0);
                            if (pid != p)
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            var index = reader.GetInt32(2);
                            return new { OID = oid, PID = pid, Index = index };
                        });

                    if (locations.Count == 0)
                    {
                        query = $"SELECT [OID], COALESCE([PID],0), [Index] FROM [dbo].[t_GeographicLocationAlias] A " +
                                 "JOIN [dbo].[t_GeographicLocation] G ON G.OID = A.GeographicLocationID " +
                                $"WHERE LOWER(A.[Alias]) = N'{n}' {NameCollation} AND G.{pid}";
                        if (preset.OID != 0)
                            query += $" AND G.[OID] = {preset.OID}";

                        locations = _connection.Select(query,
                            (reader) =>
                            {
                                var oid = reader.GetInt64(0);
                                var pid = reader.GetInt64(1);
                                var p = preset.PID.GetValueOrDefault(0);
                                if (pid != p)
                                    throw new InvalidOperationException($"Missing preset {preset.Name}");
                                //if (npid != preset.PID.Value)
                                //    throw new InvalidOperationException($"Missing preset {preset.Name}");
                                var index = reader.GetInt32(2);
                                return new { OID = oid, PID = pid, Index = index };
                            });
                    }

                    if (locations.Count == 0)
                        AddGeographicLocation(0, preset.PID.GetValueOrDefault(0), 2501, preset.Name, true);
                    else if (locations.Count > 1)
                    {
                        Log.WriteLine("Multiple presets");
                        if (Debugger.IsAttached)
                            Debugger.Break();
                    }
                }
                catch (Exception e)
                {
                    Log.WriteLine(e);
                    if (Debugger.IsAttached)
                        Debugger.Break();
                    throw;
                }
            }
        }

        private void Run(Settings.Import importInfo)
        {
            if (!File.Exists(importInfo.FilePath))
                throw new FileNotFoundException(importInfo.FilePath);

            Log.WriteLine($"Processing {importInfo.FilePath}");
            Log.Indent();
            
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
                Log.WriteLine($"Processing row {row}");
                Log.Indent();
               
                var skipRow = false;
                var allBlank = true;
                // reset, but keep any useful results from the last time through the loop
                foreach (var cd in columnDefinitions)
                {
                    var text = sheet.Cells[row, cd.ColumnNumber].Text.Trim();
                    if (!string.IsNullOrEmpty(text))
                    {
                        allBlank = false;
                        cd.CurrentValue = text;
                        cd.AssignedValue = null;
                    }
                }
                
                if (allBlank)
                {
                    dataEnded = true;
                    Log.WriteLine($"Detected end of data at row {row}");
                    break;
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
                        Debug.WriteLine("Hit breakpoint for " + cd.CurrentValue.ToString());
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
                            Log.WriteLine($"Detected end of data at row {row}");
                            break;
                        }

                        Log.WriteLine($"Error: No value for cell {cd.ColumnNumber}");
                        skipRow = true;
                    }
                }

                if (skipRow)
                {
                    Log.Outdent();
                    continue;
                }

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
                    if (cd.AssignedValue == null)
                    {
                        ProcessCell(cd);
                    }
                }

                // Now for aliases
                foreach (var cd in columnDefinitions)
                {
                    if (!string.IsNullOrEmpty(cd.CurrentValue) && (cd.AliasOf != null))
                    {
                        AddAliasIfNotPresent(cd.AliasOf.AssignedValue!, cd.CurrentValue, false);
                    }
                }

                Log.Outdent();
            }
            
            Log.Outdent();
        }

        bool ProcessCell(ColumnDefinition<GeographicLocation> coldef)
        {
            //Log.WriteLine($"Processing column {coldef.ColumnName}");
            //Log.Indent();

            // Make sure that the columns it requires have been satisfied
            if (coldef.Parent is { AssignedValue: null })
            {
                if (!ProcessCell(coldef.Parent))
                {
                    Log.WriteLine($"Error: Parent cell of {coldef.ColumnName} is empty");
                    return false;
                }
            }

            if (coldef.AliasOf is { AssignedValue: null })
            {
                if (!ProcessCell(coldef.AliasOf))
                {
                    Log.WriteLine($"Error: Predecessor cell of {coldef.ColumnName} is empty");
                    return false;
                }
            }
                
            // If the cell is empty, it must be optional (otherwise we threw an exception above)
            // Since it might still be someone's parent, we use this cell's parent as its
            // assigned location.
            if (string.IsNullOrEmpty(coldef.CurrentValue))
            {
                if (coldef.MustExist)
                    throw new InvalidOperationException("Error: Empty cell");
                if (coldef.AliasOf != null)
                    coldef.AssignedValue = coldef.AliasOf.AssignedValue;
                else if (coldef.Parent != null)
                    coldef.AssignedValue = coldef.Parent.AssignedValue;
            }
            else if (coldef.MustExist)
            {
                long parent;
                if (coldef.Parent == null)
                    parent = 20000;
                else
                    parent = coldef.Parent.AssignedValue!.Oid;
                coldef.AssignedValue = LoadGeographicLocationByName(coldef.CurrentValue, parent);
                if (coldef.AssignedValue == null)
                {
                    Log.WriteLine($"Error: Missing {coldef.CurrentValue}");
                    return false;
                }
            }
            else if (coldef.Parent != null)
            {
                var pal = coldef.Parent.AssignedValue!;
                coldef.AssignedValue = 
                    LoadGeographicLocationByName(coldef.CurrentValue, coldef.Parent.AssignedValue!.Oid) ??
                    AddGeographicLocation(0, pal.Oid, pal.PracticeAreaId, coldef.CurrentValue, coldef.IsSystemOwned);
            }

            if (coldef.AliasOf != null)
            {
                var pal = coldef.AliasOf.AssignedValue!;
                coldef.AssignedValue = pal;
                AddAliasIfNotPresent(coldef.AssignedValue, coldef.CurrentValue, false);
            }

            return true;
        }


        /// <summary>
        /// Retusn a list of ColumnDefinitions in the order they should be processed.
        /// </summary>
        /// <param name="importInfo"></param>
        /// <param name="spreadsheetIsOneBased"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        private List<ColumnDefinition<GeographicLocation>> PreprocessColumnDefinitions(Settings.Import importInfo, bool spreadsheetIsOneBased)
        {
            // Parse our column definitions
            var columnDefinitions = new SortedList<string, ColumnDefinition<GeographicLocation>>(); // columnName -> ColumnDefinition
            foreach (var icd in importInfo.Columns)
            {
                var cd = new ColumnDefinition<GeographicLocation>(icd, spreadsheetIsOneBased);
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


        private void LoadChildren(GeographicLocation parent)
        {
            // Connect to the database and load the world and continent records
            var children = _connection.Select<(long,string)>($"SELECT [OID], [Name] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parent.Oid}", 
                reader => new (reader.GetInt64(0), reader.GetString(1)));
            
            foreach (var child in children)
            {
                /*
                if (_aliases.TryGetValue(child.Item2, out var oids))
                    oids.Add(child.Item1);
                else
                    _aliases.Add(child.Item2, [child.Item1]);
                */
                var childLocation = LoadGeographicLocation(child.Item1, true);
                parent.Children.Add(child.Item2, childLocation!);
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
        private GeographicLocation? LoadGeographicLocation(long oid, bool loadAliases, int recursionLevel = 0)
        {
            var loc = GetFromCache(oid);
            if (loc == null)
            {
                //using (var command = _connection.CreateCommand($"SELECT [PID], [Name], [Index], [Description], [PracticeAreaID], [IsSystemOwned] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}"))
                {
                    var locations = _connection.Select($"SELECT [PID], [Name], [Index], [Description], [PracticeAreaID], [IsSystemOwned] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}",
                                        (reader) => {
                                            var columnIndex = 0;
                                            var pid = reader.IsDBNull(columnIndex) ? (long?)null : reader.GetInt64(columnIndex);
                                            var name = reader.GetString(++columnIndex);
                                            var index = reader.GetInt32(++columnIndex);
                                            var description = reader.GetString(++columnIndex);
                                            var paid = reader.GetInt64(++columnIndex);
                                            var isSystemOwned = reader.GetBoolean(++columnIndex);
                                            return new GeographicLocation(oid, pid, name, index, description, paid, isSystemOwned);
                                        });

                    if (locations.Count == 0)
                    {
                        Log.WriteLine($"Error: No record found for GeographicLocation {oid}");
                        return null;
                    }
                    
                    loc = locations[0];
                    AddToCache(loc);
                    Log.WriteLine($"Found location {oid} {loc.Name} in the database by OID");

                    var n = loc.Name.Replace("'", "''").ToLower();
                    _connection.ExecuteReader($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid}",
                        (reader) =>
                        {
                            var a = reader.GetString(0);
                            _aliases.Add($"{loc.Oid}{a}");
                        });
                }
            }

            if (recursionLevel-- > 0)
            {
                if (!loc!.ChildrenLoaded)
                    LoadChildren(loc);

                foreach (var child in loc!.Children.Values)
                {
                    LoadGeographicLocation(child.Oid, loadAliases, recursionLevel);
                }
            }
            
            return loc!;
        }

        
        private GeographicLocation? LoadGeographicLocationByName(string name, long? pid)
        {
            /*
            if ((pid != null) && _aliases.TryGetValue(name, out var oids))
            {
                foreach (var o in oids)
                {
                    var loc = LoadGeographicLocation(o, false);
                    if (loc.Pid == pid)
                        return loc;
                }
            }
            */

            var loc = GetFromCache(pid ?? 0, name);
            if (loc != null)
                return loc;

            var n = name.Replace("'", "''").ToLower();
            var query1 = $"SELECT V.[OID] FROM [dbo].[t_GeographicLocation] V " +
                        $"WHERE LOWER(V.[Name]) = N'{n}' {NameCollation}";
            if ((pid == null) || (pid.Value == 0))
                query1 += $" AND V.[PID] IS NULL";
            else
                query1 += $" AND V.[PID] = {pid.Value}";

            var locations = _connection.Select(query1, (reader) => reader.GetInt64(0));

            if (locations.Count == 0)
            {
                var query2 = $"SELECT G.[OID] FROM [dbo].[t_GeographicLocationAlias] V " +
                             $"JOIN [dbo].[t_GeographicLocation] G ON (G.OID = V.GeographicLocationID) " +
                             $"WHERE LOWER(V.[Alias]) = N'{n}' {NameCollation}";
                if ((pid == null) || (pid.Value == 0))
                    query2 += $" AND G.[PID] IS NULL";
                else
                    query2 += $" AND G.[PID] = {pid.Value}";
                locations = _connection.Select(query2, (reader) => reader.GetInt64(0));
            }

            if (locations.Count == 0)
            {
                Log.WriteLine($"Did not find location {name} in the database by Name");
                return null;
            }
            else if (locations.Count > 1)
            {
                var s = string.Join(", ", locations);
                Log.WriteLine($"Conflicting records found for location {name}: {s}");
                return null;
            }

            var location = LoadGeographicLocation(locations[0], true);
            // Log.WriteLine($"Found location {oid} {location!.Name} in the database by Name"); // already reported
            return location!;
        }

        private GeographicLocation AddGeographicLocation(long oid, long pid, long? practiceAreaId, string name, bool isSystemOwned)
        {
            if (oid < 1)
                oid = _connection.GetNextOid();
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
                var query = $"INSERT [dbo].[t_GeographicLocation] ([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorID],[PracticeAreaID],[CreationSession]) " +
                            $"VALUES({oid}, {pidstr}, {iso}, N'{n}', {index}, N'{d}', '{_startTime}', {_settings.CreatorId}, {practiceAreaId}, '{_creationSession}')";
                var result = _connection.ExecuteNonQuery(query);
                if (result != 1)
                    throw new InvalidOperationException($"Insert failed for {name}");
                Log.WriteLine($"Added location {oid} {name} to database.");
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
            AddToCache(location);
            parent?.Children.Add(name, location);

            AddAliasIfNotPresent(location, name, true);
            return location;
        }
        
        private bool AddAliasIfNotPresent(GeographicLocation location, string alias, bool isPrimary)
        {
            if (_aliases.Contains($"{location.Oid}{alias}"))
                return true;

            alias = alias.Replace("'", "''");
            var a = alias.ToLower();
            try
            {
                var query = $"SELECT COUNT(*) FROM [dbo].[t_GeographicLocationAlias] " +
                            $"WHERE LOWER([Alias]) = N'{a}' {AliasCollation} AND [GeographicLocationID] = {location.Oid}";
                var result = _connection.ExecuteScalar(query);
                if ((result is long and > 0))
                {
                    _aliases.Add($"{location.Oid}{alias}");
                    Log.WriteLine($"Found alias {alias} for {location.Oid} {location.Name}");
                    return true; // it worked
                }
                if ((result is int and > 0))
                {
                    _aliases.Add($"{location.Oid}{alias}");
                    Log.WriteLine($"Found alias {alias} for {location.Oid} {location.Name}");
                    return true; // it worked
                }
            }
            catch (Exception e)
            {
                Log.WriteLine(e);
                return false;
            }
             
            var aliasOid = _connection.GetNextOid();
            var description = alias;
            var d = description.Replace("'", "''");
            var practiceAreaId = location.PracticeAreaId;
            var isPrimaryInt = isPrimary ? 1 : 0;
            var isSystemOwnedInt = location.IsSystemOwned ? 1 : 0;
            //using (var command = _connection.CreateCommand($"INSERT [dbo].[t_GeographicLocationAlias] ([OID],[GeographicLocationID],[Alias],[Description],[IsPrimary],[PracticeAreaID],[CreationDate],[CreatorID],[CreationSession],[LID],[IsSystemOwned]) " +
            //                                   $"VALUES({aliasOid}, {location.Oid}, N'{a}', N'{d}', {isPrimaryInt}, {practiceAreaId}, '{_startTime}', {_settings.CreatorId}, '{_creationSession}', 500, {isSystemOwnedInt})"))
            { 
                var result = _connection.ExecuteNonQuery($"INSERT [dbo].[t_GeographicLocationAlias] ([OID],[GeographicLocationID],[Alias],[Description],[IsPrimary],[PracticeAreaID],[CreationDate],[CreatorID],[CreationSession],[LID],[IsSystemOwned]) " +
                                               $"VALUES({aliasOid}, {location.Oid}, N'{alias}', N'{d}', {isPrimaryInt}, {practiceAreaId}, '{_startTime}', {_settings.CreatorId}, '{_creationSession}', 500, {isSystemOwnedInt})");
                if (result != 1)
                {
                    Log.WriteLine($"Failed to insert alias {alias} for {location.Oid} {location.Name}");
                    return false;
                }
            }

            //if (_aliases.TryGetValue(alias, out existing))
            //    existing.Add(location.Oid);
            //else
            //    _aliases.Add(alias, [location.Oid]);
            _aliases.Add($"{location.Oid}{alias}");
            Log.WriteLine($"Inserted alias {alias} for {location.Oid} {location.Name}");
            return true;
        }
        
        private void Dump()
        {
            var oldLog = Log.Enabled;
            Log.Enabled = false;
            const string filename = "dump.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using var file = File.CreateText(filename);
            Dump(0, "", file, new HashSet<long>());
            Log.Enabled = oldLog;
        }

        private void Dump(long pid, string indent, StreamWriter file, HashSet<long> pids)
        {
            if (!pids.Add(pid))
                throw new Exception();
            var p = (pid == 0) ? "G.[PID] IS NULL" : ("G.[PID] = " + pid);
            var names = _connection.Select("SELECT G.[OID], A.Alias FROM [dbo].[t_GeographicLocationAlias] A " +
                                            "JOIN [dbo].[t_GeographicLocation] G ON G.OID = A.GeographicLocationID " +
                                            $"WHERE {p} ORDER BY G.[OID], A.[IsPrimary], A.[Alias]", 
                reader => new { oid = reader.GetInt64(0), name = reader.GetString(1) });

            if (names.Count == 0) 
                return;

            // Write the first name
            var j = 0;
            file.Write($"{indent}{names[j].oid} {names[j].name}");

            // Write the other names
            var lastChildrenDumped = 0L;
            for (++j; j < names.Count; ++j)
            {
                // if it's another name for the same OID we just emitted, write it on the same line
                if (names[j].oid == names[j - 1].oid)
                {
                    file.Write($", {names[j].name}");
                }
                else // it's the name of another OID
                {
                    file.WriteLine(); // finish the line...
                    lastChildrenDumped = names[j - 1].oid;
                    Dump(names[j - 1].oid, indent + "    ", file, pids); //... and dump the OID's children
                    // Write the name
                    file.Write($"{indent}{names[j].oid} {names[j].name}");
                }
            }

            file.WriteLine(); // finish the last name
            if (lastChildrenDumped != names[j-1].oid)
                Dump(names[j - 1].oid, indent + "    ", file, pids); //... and dump the OID's children
        }

        private void AddToCache(GeographicLocation geographicLocation)
        {
            _locationCacheByOid.Add(geographicLocation.Oid, geographicLocation);
            _locationCacheByName.Add($"{geographicLocation.Pid ?? 0}{geographicLocation.Name}", geographicLocation);
        }

        private GeographicLocation? GetFromCache(long oid)
        {
            return _locationCacheByOid.TryGetValue(oid, out var geographicLocation) ? geographicLocation : null;
        }

        private GeographicLocation? GetFromCache(long pid, string name)
        {
            return _locationCacheByName.TryGetValue($"{pid}{name}", out var geographicLocation) ? geographicLocation : null;
        }
    }
}
