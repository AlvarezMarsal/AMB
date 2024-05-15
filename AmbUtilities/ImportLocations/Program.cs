using OfficeOpenXml;
using System.Data.SqlClient;

namespace ImportLocations
{
    internal class Program
    {
        private readonly DateTime _startTime = DateTime.Now;
        private readonly Guid _creationSession = new ();
        private long _creatorId;
        private SqlConnection? _connection;
        private readonly HashSet<long> _geographicLocationOidsInUse = [];
        private GeographicLocation? _world;
        private SortedList<GeographicLocationKind, SortedList<string, GeographicLocation>> _aliases = [];

        static void Main(string[] args)
        {
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
        }

        private void Run(string[] args)
        {
            if (!long.TryParse(args[5], out _creatorId))
                throw new ArgumentException("Invalid creatorId");

            // Connect to the database and load the world and continent records
            if (args[3] == ".")
                args[3] = Environment.MachineName;
            _connection = new SqlConnection($"Server={args[3]};Database={args[4]};Integrated Security=True;");
            using (_connection)
            {
                _connection.Open();
                LoadWorld();
                LoadAliases(_world!);
            }
            
            // Process the template (the excel spreadsheet)
            var argIndex = 0;
            var excelFilename = args[argIndex++];
            if (!File.Exists(excelFilename))
                throw new FileNotFoundException(excelFilename);
            var sheetName = args[argIndex++];
            var rangeName = args[argIndex];

            using var package = new ExcelPackage(new FileInfo(excelFilename));
            var zero = package.Compatibility.IsWorksheets1Based ? 1 : 0;
            using var sheet = package.Workbook.Worksheets[sheetName];

            if (!ParseAddress(rangeName, out var top, out var left, out var bottom, out var right))
                throw new ArgumentException("Invalid range");

            // Examine each row
            for (var row=top; row<=bottom; ++row)
            {
                var column = left;
                var continentName = sheet.Cells[row, column++].Text.Trim();
                var country = sheet.Cells[row, column++].Text.Trim();
                var alias1 = sheet.Cells[row, column++].Text.Trim();
                var alias2 = sheet.Cells[row, column++].Text.Trim();
                var state = sheet.Cells[row, column++].Text.Trim();
                var city = sheet.Cells[row, column++].Text.Trim();
                var cityAscii = sheet.Cells[row, column].Text.Trim();

                if (!_aliases[GeographicLocationKind.Continent].TryGetValue(continentName, out var continent))
                {
                    if (!_world!.Children.TryGetValue(continentName, out var continent))
                    {
                        continent = AddGeographicLocation(_world, continentName);
                        Console.WriteLine($"Continent {continentName} added");
                    }

                    /*
                    if (_aliases[GeographicLocationKind.Country].TryGetValue(country, out var countryEntity))
                    {
                        if (_aliases[GeographicLocationKind.Region].TryGetValue(state, out var stateEntity))
                        {
                            if (_aliases[GeographicLocationKind.City].TryGetValue(city, out var cityEntity))
                            {
                                if (cityEntity.Parent != stateEntity)
                                {
                                    Console.WriteLine($"City {city} is not in state {state}");
                                }
                            }
                            else
                            {
                                cityEntity = AddGeographicLocation(stateEntity, city);
                                Console.WriteLine($"City {city} added");
                            }
                        }
                        else
                        {
                            stateEntity = AddGeographicLocation(countryEntity, state);
                            Console.WriteLine($"State {state} added");
                            var cityEntity = AddGeographicLocation(stateEntity, city);
                            Console.WriteLine($"City {city} added");
                        }
                    }
                    else
                    {
                        countryEntity = AddGeographicLocation(continent, country);
                        Console.WriteLine($"Country {country} added");
                        var stateEntity = AddGeographicLocation(countryEntity, state);
                        Console.WriteLine($"State {state} added");
                        var cityEntity = AddGeographicLocation(stateEntity, city);
                        Console.WriteLine($"City {city} added");
                    }
                }
                else
                {
                    continent = AddGeographicLocation(_world, continentName);
                    Console.WriteLine($"Continent {continentName} added");
                    var countryEntity = AddGeographicLocation(continent, country);
                    Console.WriteLine($"Country {country} added");
                    var stateEntity = AddGeographicLocation(countryEntity, state);
                    Console.WriteLine($"State {state} added");
                    var cityEntity = AddGeographicLocation(stateEntity, city);
                    Console.WriteLine($"City {city} added");
                }
                    */
                }
            }
        }

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
            _world.Walk(entity => _geographicLocationOidsInUse.Add(entity.Oid));
        }
        
 
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


        private void LoadChildren(GeographicLocation parent)
        {
            // Connect to the database and load the world and continent records
            using var command = new SqlCommand($"SELECT [OID],[Name],[Index],[Description],[PracticeAreaID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parent.Oid}", _connection);
            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var columnIndex = 0;
                var oid = reader.GetInt64(columnIndex++);
                var name = reader.GetString(columnIndex++);
                var index = reader.GetInt32(columnIndex++);
                var description = reader.GetString(columnIndex++);
                var paid = reader.GetInt64(columnIndex);
                var child = new GeographicLocation(parent, oid, name, index, description, paid);
                parent.Children.Add(name, child);
            }
        }

        private GeographicLocation AddGeographicLocation(GeographicLocation parent, string name)
        {
            if (parent.Children.Count == 0)
                throw new InvalidOperationException("Parent has no children");

            var oid = parent.Children.Values.Max(entity => entity.Oid) + 1;
            while (_geographicLocationOidsInUse.Contains(oid))
                ++oid;
            var index = parent.Children.Values.Max(entity => entity.Index) + 1;
            var description = name + "-RS-Test";
            var practiceAreaId = parent.PracticeAreaId;

            // Connect to the database and load the world and continent records
            using var command = new SqlCommand($"INSERT [dbo].[t_GeographicLocation] ([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorID],[PracticeAreaID],[CreationSession]) " +
                                               $"VALUES({oid},{parent.Oid},1,'{name}',{index},'{description}','{_startTime}',{_creatorId},{practiceAreaId},'{_creationSession}')", _connection);
            var result = command.ExecuteNonQuery();
            if (result != 1)
                throw new InvalidOperationException($"Insert failed for {name}");
            parent.Children.Add(name, new GeographicLocation(parent, oid, name, index, description, practiceAreaId));
            _geographicLocationOidsInUse.Add(oid);
            return new GeographicLocation(parent, oid, name, index, description, parent.PracticeAreaId);
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
    }
}
