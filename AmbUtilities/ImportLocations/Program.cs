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
        private readonly SortedList<string, List<long>> _aliases = [];

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
            using (_connection)
            {
                _connection.Open();

                EnforcePresets();

                foreach (var importFile in _settings.ImportFiles)
                {
                    Run(importFile);
                }
            }
        }

        private void EnforcePresets()
        {
            foreach (var preset in _settings.Presets)
            {
                var pidPhrase = (preset.Pid == null) ? "IS NULL" : $"= {preset.Pid}";
                using var command = new SqlCommand($"SELECT COUNT(*) FROM [dbo].[t_GeographicLocationAlias] A " +
                                                   $"JOIN [dbo].[t_GeographicLocation] L ON A.[GeographicLocationID] = L.[OID] " +
                                                   $"WHERE A.[GeographicLocationID] = {preset.Oid} AND A.[Alias] = '{preset.Name}' AND L.PID {pidPhrase}",
                                                   _connection);
                var result = command.ExecuteScalar();
                if ((result is long and < 1))
                {
                    Debug.WriteLine($"Missing preset {preset.Name} {preset.Oid} {preset.Pid}");
                    //AddGeographicLocation(preset);
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

            if (!ParseAddress(importInfo.Range, out var top, out var left, out var bottom, out var right))
                throw new ArgumentException("Invalid range");

            // Examine each row
            for (var row=top; row<=bottom; ++row)
            {
                // We work our way DOWN the hierarchy, making sure each level is in the database
                GeographicLocation? parent = null;
                for (var i=importInfo.Hierarchy.Count - 1; i >= 0; --i)
                {
                    var hierarchyInfo = importInfo.Hierarchy[i];
                    if (hierarchyInfo.Oid.HasValue)
                    {
                        if (hierarchyInfo.Column != null)
                            throw new InvalidOperationException($"Invalid hierarchy entry: {hierarchyInfo}");
                        parent = LoadGeographicLocation(hierarchyInfo.Oid.Value, 1);
                    }
                    else if (hierarchyInfo.Column != null)
                    {
                        var col = ColumnAlphaToColumnNumber(hierarchyInfo.Column, isOneBased);
                        var parentAlias = sheet.Cells[row, col].Text.Trim();
                    }
                    else
                    {
                        throw new InvalidOperationException($"Invalid hierarchy entry: {hierarchyInfo}");
                    }

                        var column = left;
                    var parentName = sheet.Cells[row, column++].Text.Trim();
                    var parentOid = hierarchyInfo.Oid;
                    var parentPid = hierarchyInfo.Pid;
                    var parentAliases = hierarchyInfo.Aliases;

                    if (!_aliases.TryGetValue(GeographicLocationKind.City, out var aliasesByKind))
                        _aliases.Add(GeographicLocationKind.City, aliasesByKind = new SortedList<string, GeographicLocation>());
                    if (aliasesByKind.TryGetValue(parentName, out var parent))
                    {
                        if (parent.Oid != parentOid)
                            throw new InvalidOperationException($"Duplicate oid {parentOid}");
                    }
                    else
                    {
                        parent = new GeographicLocation(parentOid, parentPid, parentName, 0, parentName, 2501);
                        aliasesByKind.Add(parentName, parent);
                    }

                    foreach (var alias in parentAliases)
                    {
                        if (aliasesByKind.TryGetValue(alias, out var existing))
                        {
                            if (existing.Oid != parentOid)
                                throw new InvalidOperationException($"Duplicate oid {parentOid}");
                        }
                        else
                        {
                            aliasesByKind.Add(alias, parent);
                        }
                    }
                }
                importInfo.Name;
                importInfo.Aliases;

                var column = left;
                var continentName = sheet.Cells[row, column++].Text.Trim();
                var countryName = sheet.Cells[row, column++].Text.Trim();
                var countryAlias1= sheet.Cells[row, column++].Text.Trim();
                var countryAlias2= sheet.Cells[row, column++].Text.Trim();
                var regionName = sheet.Cells[row, column++].Text.Trim();
                var cityName = sheet.Cells[row, column++].Text.Trim();
                var cityAscii = sheet.Cells[row, column].Text.Trim();

                if (!_aliases[GeographicLocationKind.Continent].TryGetValue(continentName, out var continent))
                {
                    continent = AddGeographicLocation(_world!, continentName);
                    Console.WriteLine($"Continent {continentName} added");
                }

                if (!_aliases[GeographicLocationKind.Country].TryGetValue(countryName, out var country))
                {
                    country = AddGeographicLocation(continent, countryName, countryAlias1, countryAlias2);
                    Console.WriteLine($"Country {countryName} added");
                }

                if (!_aliases[GeographicLocationKind.State].TryGetValue(regionName, out var region))
                {
                    region = AddGeographicLocation(country, regionName);
                    Console.WriteLine($"Region {regionName} added");
                }

                if (!_aliases[GeographicLocationKind.City].TryGetValue(cityName, out var city))
                {
                    city = AddGeographicLocation(region, cityName, cityAscii);
                    Console.WriteLine($"City {cityName} added");
                }
            }
        }

        private static int ColumnAlphaToColumnNumber(string alpha, bool isOneBased)
        {
            var result = 0;
            foreach (var ch in alpha)
            {
                if (!char.IsLetter(ch))
                    break;
                if (char.IsLower(ch))
                    throw new ArgumentException("Invalid column name");
                result = result * 26 + ch - 'A' + 1;
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
                using (var command = new SqlCommand($"SELECT [OID],[PID],[Name],[Index],[Description],[PracticeAreaID] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}", _connection))
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
                    var paid = reader.GetInt64(columnIndex);
                    location = new GeographicLocation(oid, pid, name, index, description, paid);
                    _locations.Add(oid, location);
                }

                using (var command = new SqlCommand($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAliases] WHERE [GeographicLocationID] = {oid}", _connection))
                {
                    using var reader = command.ExecuteReader();
                    if (!reader.Read())
                        throw new InvalidOperationException($"No alias record found for {oid}");
                    var alias = reader.GetString(0);

                    if (!_aliases.TryGetValue(alias, out var existing))
                        _aliases.Add(alias, existing = new List<long>());
                    if (!existing.Contains(oid))
                    {
                        existing.Add(oid);
                        if (existing.Count > 1)
                            existing.Sort();
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


        private long AddGeographicLocation(long? oid, long? pid, long? practiceAreaId, string name, params string[] aliases)
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
            var pidstr = (pid == null) ? "NULL" : pid.ToString();
            using (var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocation] ([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorID],[PracticeAreaID],[CreationSession]) " +
                                               $"VALUES({oid},{pidstr},1,'{name}',{index},'{description}','{_startTime}',{_settings.CreatorId},{practiceAreaId},'{_creationSession}')", _connection))
            {
                var result = command.ExecuteNonQuery();
                if (result != 1)
                    throw new InvalidOperationException($"Insert failed for {name}");
            }

            var location = new GeographicLocation(oid.Value, pid, name, index, description, practiceAreaId.Value);
            _locations.Add(oid.Value, location);
            parent?.Children.Add(name, location);

            AddAlias(location, name, true);
            foreach (var alias in aliases)
            {
                AddAlias(location, alias, false);
            }
            
            return oid.Value;
        }
        
        private void AddAlias(GeographicLocation location, string alias, bool isPrimary)
        {
            if (_aliases.TryGetValue(alias, out var existing))
            {
                if (existing.Contains(location.Oid))
                    return;
                existing.Add(location.Oid);
                if (existing.Count > 1)
                    existing.Sort();
            }
            else
            {
                _aliases.Add(alias, existing = new List<long> { location.Oid });
            }

            using (var command = new SqlCommand($"SELECT COUNT(*) FROM [dbo].[t_GeographicLocationAliases] WHERE [Alias] = '{alias}' AND [GeographicLocationID] = {location.Oid}", _connection))
            {
                var result = command.ExecuteScalar();
                if ((result is long and > 0))
                    return;
            }
            
            var aliasOid = GetNextOid();
            var description = alias + "-RS-Test";
            var practiceAreaId = location.PracticeAreaId;

            using (var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocationAlias] ([OID],[GeographicLocationID],[Alias],[Description],[IsPrimary],[PracticeAreaID],[CreationDate],[CreatorID],[CreationSession],[LID]) " +
                                               $"VALUES({aliasOid},{location.Oid},'{alias}','{description}',{isPrimary},{practiceAreaId},'{_startTime}',{_settings.CreatorId},'{_creationSession}', 500)", _connection))
            { 
                var result = command.ExecuteNonQuery();
                if (result != 1)
                    throw new InvalidOperationException($"Insert failed for {alias}");
            }
        }
        
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

        private long GetNextOid()
        {
            using var command = new SqlCommand("sp_GetNextOID", _connection);
            command.CommandType = CommandType.StoredProcedure; 
            var result = command.ExecuteScalar();
            if (result is long oid)
                return oid;
            throw new InvalidOperationException("No OID returned");
        }
    }
}
