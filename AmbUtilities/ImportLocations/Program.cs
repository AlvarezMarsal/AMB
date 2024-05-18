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
        //private GeographicLocation? _world;
        private readonly SortedList<string, HashSet<long>> _aliases = [];

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



            /*
            if (args.Length == 1)
                settingsFileName = args[0];
            else if (args.Length == 0)
                settingsFileName = "importLocations.json";
            var settings = new Settings();
            settings.ConnectionString = "Server=.;Database=AMBenchmark_DB;Integrated Security=True;";
            settings.ImportFiles.Add(new Settings.ImportFileInfo { FilePath = @"D:\AMB\World Cities.xlsx" });
            var json = System.Text.Json.JsonSerializer.Serialize(settings);
            File.WriteAllText("importLocations.json", json);


            if (args.Length != 6)
            {
                Console.WriteLine("ImportLocations excelFileName sheetName rangeName server database creatorId");
                return;
            }

            try
            {
                var program = new Program();
                program.Run(args);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            */
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
                _connection.Open();

                EnforcePresets();

                foreach (var importFile in _settings.ImportFiles)
                {
                    Run(importFile);
                }

                Dump();
            }
        }

        private void EnforcePresets()
        {
            foreach (var preset in _settings.Presets)
            {
                var pidPhrase = (preset.Pid == null) ? "IS NULL" : $"= {preset.Pid}";
                var query = $"SELECT COUNT(*) FROM [dbo].[t_GeographicLocationAlias] A " +
                            $"JOIN [dbo].[t_GeographicLocation] L ON A.[GeographicLocationID] = L.[OID] " +
                            $"WHERE A.[Alias] = N'{preset.Name}' AND L.PID {pidPhrase}";
                if (preset.Oid != 0)
                    query += $" AND A.[GeographicLocationID] = {preset.Oid}";
                
                using var command = new SqlCommand(query, _connection);
                var result = command.ExecuteScalar();
                if (result is long and < 1 or int and < 1)
                {
                    Debug.WriteLine($"Missing preset {preset.Name} {preset.Oid} {preset.Pid}");
                    if (preset.Oid != 0)
                        throw new InvalidOperationException($"Missing preset {preset.Name}");
                    AddGeographicLocation(GetNextOid(), preset.Pid, 2501, preset.Name);
                }
            }
        }

        private void Run(Settings.ImportFileInfo importInfo)
        {
            /*
            LoadWorld();
            LoadAliases(_world!);
            */
            /*
                // Process the template (the excel spreadsheet)
                var argIndex = 0;
                var excelFilename = args[argIndex++];
            */
            if (!File.Exists(importInfo.FilePath))
                throw new FileNotFoundException(importInfo.FilePath);
            //var sheetName = args[argIndex++];
            //var rangeName = args[argIndex];

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
                var skipRow = false;
                // reset, but keep any useful results from the last time through the loop
                foreach (var cd in columnDefinitions)
                {
                    cd.CurrentValue = sheet.Cells[row, cd.ColumnNumber].Text.Trim();
                    if (cd.CurrentValue != cd.AssignedGeographicLocation?.Name)
                        cd.AssignedGeographicLocation = null;

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

            Dump();
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
                        AddGeographicLocation(null, pal.Oid, pal.PracticeAreaId, coldef.CurrentValue);
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
            /*
            // Set up for generation indexing
            var orderedByParentage = new List<ColumnDefinition>(columnDefinitions.Count);
            var columnsNotProcessed = new HashSet<ColumnDefinition>(); // column numbers
            foreach (var cd in columnDefinitions.Values)
            {
                if (cd.SettingsDefinition.MustExist)
                    orderedByParentage.Add(cd);
                else
                    columnsNotProcessed.Add(cd);
            }

            if (orderedByParentage.Count == 0)
                throw new InvalidOperationException("No columns marked as 'MustExist'");

            while (true)
            {
                var notProcessed = columnsNotProcessed.ToList();
                if (notProcessed.Count == 0)
                    break;
                var count = notProcessed.Count;

                foreach (var cd in notProcessed)
                {
                    var ready = ((cd.Parent == null) || orderedByParentage.Contains(cd.Parent)) &&
                        ((cd.AliasOf == null) || orderedByParentage.Contains(cd.AliasOf));
                    if (ready)
                    {
                        orderedByParentage.Add(cd);
                        columnsNotProcessed.Remove(cd);
                    }
                }

                if (columnsNotProcessed.Count == count)
                    throw new InvalidOperationException("Circular reference in column definitions");
            }

            // The 'orderedByParentage' list is now in the reverse of the order
            // we want to process the columns.
            //orderedByParentage.Reverse();
            return orderedByParentage;
            */
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

        /*
        private void LoadWorld()
        {
            using var command = new SqlCommand("SELECT [OID],[Name],[Index],[Description],[PracticeAreaID] FROM [dbo].[t_GeographicLocation] WHERE [PID] IS NULL", _connection);
            using (var reader = command.ExecuteReader())
            {
                var result = reader.Read();
                if (!result)
                    throw new InvalidOperationException("No world record found");

                var columnIndex = 0;
                var oid = reader.GetInt64(columnIndex++);
                var name = reader.GetString(columnIndex++);
                var index = reader.GetInt32(columnIndex++);
                var description = reader.GetString(columnIndex++);
                var paid = reader.GetInt64(columnIndex);
                _world = new GeographicLocation(null, oid, name, index, description, paid);
            }
            
            LoadDescendants(_world); 
        }
        */
        /*
        private void LoadDescendants(GeographicLocation parent, int generations = int.MaxValue)
        {
            var currentGeneration = new List<GeographicLocation> { parent };
            while (generations-- > 0)
            {
                var nextGeneration = new List<GeographicLocation>();
                foreach (var entity in currentGeneration)
                {
                    LoadChildren(entity);
                    nextGeneration.AddRange(entity.Children.Values);
                }
                currentGeneration = nextGeneration;
                if (currentGeneration.Count == 0)
                    break;
            }
        }

        private void LoadAliases(GeographicLocation entity)
        {
            if (!_aliases.TryGetValue(entity.Kind, out var aliasesByKind))
                _aliases.Add(entity.Kind, aliasesByKind = new SortedList<string, GeographicLocation>());
            if (aliasesByKind.TryGetValue(entity.Name, out var existing))
            {
                if (existing.Oid != entity.Oid)
                    throw new InvalidOperationException($"Duplicate oid {entity.Oid}");
            }
            else
            {
                aliasesByKind.Add(entity.Name, entity);
            }

            var command = new SqlCommand($"SELECT [OID],[Alias],[Description],[IsPrimary],[PracticeAreaID] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {entity.Oid}", _connection);
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var columnIndex = 0;
                    var oid = reader.GetInt64(columnIndex++);
                    var alias = reader.GetString(columnIndex++);
                    var description = reader.GetString(columnIndex++);
                    var isPrimary = reader.GetBoolean(columnIndex++);
                    var paid = reader.GetInt64(columnIndex);

                    if (aliasesByKind.TryGetValue(alias, out existing))
                    {
                        if (existing.Oid != entity.Oid)
                            throw new InvalidOperationException($"Duplicate oid {entity.Oid}");
                    }
                    else
                    {
                        aliasesByKind.Add(alias, entity);
                    }
                }
            }

            foreach (var child in entity.Children.Values)
            {
                LoadAliases(child);
            }
        }

        */

        private void LoadChildren(GeographicLocation parent)
        {
            // Connect to the database and load the world and continent records
            var oids = new List<long>();
            using (var command = new SqlCommand($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parent.Oid}", _connection))
            {
                using var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    oids.Add(reader.GetInt64(0));
                }
            }

            foreach (var oid in oids)
            {
                var child = LoadGeographicLocation(oid);
                parent.Children.TryAdd(child.Name, child);
            }

            parent.ChildrenLoaded = true;
        }

        private GeographicLocation LoadGeographicLocation(long oid, int recursionLevel = 0)
        {
            if (!_locations.TryGetValue(oid, out var location))
            {
                using (var command = new SqlCommand($"SELECT [OID],[PID],[Name],[Index],[Description],[PracticeAreaID],[IsSystemOwned] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}", _connection))
                {
                    using var reader = command.ExecuteReader();
                    if (!reader.Read())
                        throw new InvalidOperationException($"No record found for {oid}");
                    var columnIndex = 0;
                    /*var oid = reader.GetInt64(*/ columnIndex++ /*)*/;
                    var pid = reader.IsDBNull(columnIndex) ? (long?) null : reader.GetInt64(columnIndex);
                    ++columnIndex;
                    var name = reader.GetString(columnIndex++);
                    var index = reader.GetInt32(columnIndex++);
                    var description = reader.GetString(columnIndex++);
                    var paid = reader.GetInt64(columnIndex++);
                    var isSystemOwned = reader.GetBoolean(columnIndex);
                    location = new GeographicLocation(oid, pid, name, index, description, paid, isSystemOwned);
                    _locations.Add(oid, location);
                    if (!_aliases.TryGetValue(location.Name, out var existingAliases))
                        _aliases.Add(location.Name, [oid]);
                    else
                        existingAliases.Add(oid);
                }

                using (var command = new SqlCommand($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid}", _connection))
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
            }

            if (recursionLevel-- > 0)
            {
                LoadChildren(location);
                foreach (var child in location.Children.Values)
                {
                    LoadGeographicLocation(child.Oid, recursionLevel);
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
                    var location = LoadGeographicLocation(o);
                    if (location.Pid == pid)
                        return location;
                }
            }

            var n = name.Replace("'", "''");
            var query = $"SELECT A.[GeographicLocationID] FROM [dbo].[t_GeographicLocationAlias] A " +
                        $"JOIN [dbo].[t_GeographicLocation] L ON A.[GeographicLocationID] = L.[OID] " +
                        $"WHERE A.[Alias] = N'{n}'";
            if (pid != null)
                query += $" AND L.[PID] = {pid}";

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
                        throw new InvalidOperationException($"Conflicting records found for records found for {name}");
                }
            }
            return LoadGeographicLocation(oid);
        }

        private GeographicLocation AddGeographicLocation(long? oid, long? pid, long? practiceAreaId, string name)
        {
            oid ??= GetNextOid();
            var parent = (pid == null) ? null : LoadGeographicLocation(pid.Value, 1); // includes children
            int index;
            if (parent == null)
            {
                index = 0;
                practiceAreaId ??= 2501;
            }
            else
            {
                index = (parent.Children.Count == 0) ? 0 : parent.Children.Values.Max(entity => entity.Index) + 1;
                practiceAreaId ??= parent.PracticeAreaId;
            }

            var description = name + "-RS-Test";

            // Connect to the database and load the world and continent records
            try
            {
                var pidstr = (pid == null) ? "NULL" : pid.ToString();
                var n = name.Replace("'", "''");
                var d = description.Replace("'", "''");
                using var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocation] ([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorID],[PracticeAreaID],[CreationSession]) " +
                                                   $"VALUES({oid}, {pidstr}, 1, N'{n}', {index}, N'{d}', '{_startTime}', {_settings.CreatorId}, {practiceAreaId}, '{_creationSession}')", _connection);
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

            var location = new GeographicLocation(oid.Value, pid, name, index, description, practiceAreaId.Value, true); // TODO : isSystemOwned
            _locations.Add(oid.Value, location);
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
                using var command = new SqlCommand($"SELECT COUNT(*) FROM [dbo].[t_GeographicLocationAlias] WHERE [Alias] = N'{a}' AND [GeographicLocationID] = {location.Oid}", _connection);
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
            var description = a + "-RS-Test";
            var practiceAreaId = location.PracticeAreaId;
            var isPrimaryInt = isPrimary ? 1 : 0;
            var isSystemOwnedInt = location.IsSystemOwned ? 1 : 0;
            using (var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocationAlias] ([OID],[GeographicLocationID],[Alias],[Description],[IsPrimary],[PracticeAreaID],[CreationDate],[CreatorID],[CreationSession],[LID],[IsSystemOwned]) " +
                                               $"VALUES({aliasOid},{location.Oid},N'{a}',N'{description}',{isPrimaryInt},{practiceAreaId},'{_startTime}',{_settings.CreatorId},'{_creationSession}', 500, {isSystemOwnedInt})", _connection))
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
        
        /*
        private static bool ParseAddress(string address, out int row, out int column)
        {
            var a = new ExcelAddressBase(address);
            row = a.Start.Row;
            column = a.Start.Column;
            return true;
        }

        private static bool ParseAddress(string address, out int top, out int left, out int bottom, out int right)
        {
            var a = new ExcelAddressBase(address);
            top = a.Start.Row;
            left = a.Start.Column;
            bottom = a.End.Row;
            right = a.End.Column;
            return true;
        }
        */

        private long GetNextOid()
        {
            using var command = new SqlCommand("[dbo].sp_internalGetNextOID", _connection);
            command.CommandType = CommandType.StoredProcedure; 
            command.Parameters.Add("@oid", SqlDbType.BigInt).Direction = ParameterDirection.Output;
            command.ExecuteNonQuery();
            return Convert.ToInt64(command.Parameters["@oid"].Value);
        }

        private IReadOnlyList<T> Select<T>(string query, Func<IDataReader, T> build)
        {
            var list = new List<T>();
            using (var command = new SqlCommand(query, _connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var t = build(reader);
                        list.Add(t);
                    }
                }
            }
            return list;
        }

        private void Dump()
        {
            var filename = "dump.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using var file = File.CreateText(filename);

            var worlds = Select("SELECT [OID],[Name] FROM [dbo].[t_GeographicLocation] WHERE [PID] IS NULL",
                reader => new { OID = reader.GetInt64(0), Name = reader.GetString(1) });
            foreach (var world in worlds)
            {
                file.WriteLine($"{world.Name} ({world.OID})");
                DumpChildren(world.OID, 1, file);
            }
        }

        private void DumpChildren(long oid, int indent, StreamWriter file)
        {
            var children = Select($"SELECT [OID],[Name] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {oid}",
                reader => new { OID = reader.GetInt64(0), Name = reader.GetString(1) });
            foreach (var child in children)
            {
                for (var i=0; i < indent; ++i)
                    file.Write("\t");
                file.WriteLine($"{child.Name} ({child.OID})");
                DumpChildren(child.OID, indent+1, file);
            }
        }
    }
}
