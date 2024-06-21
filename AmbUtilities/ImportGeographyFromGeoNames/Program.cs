using System.Diagnostics;
using System.Data;
using AmbHelper;
using System.Globalization;


namespace ImportGeographyFromGeoNames;

internal partial class Program
{
    private static readonly Dictionary<string, Country> Countries = new (StringComparer.OrdinalIgnoreCase);
    private static readonly Dictionary<long, Entry> AreasById = new ();
    private static string? _server = ".";
    private static string? _database = "AMBenchmark_DB";
    private static bool _dump = false;
    private static bool _keep = false;
    private static bool _continue = false;
    private static AmbDbConnection? _connection;
    private static readonly DateTime CreationDate = DateTime.Now;
    private static readonly string CreationDateAsString;
    private static long _creatorId = 100;
    private static int _practiceAreaId = 2501;
    private static readonly Guid CreationSession = Guid.NewGuid();
    private static readonly string CreationSessionAsString;
    private static long _populationCutoff = 1000;
    private static Dictionary<string, string> CountryToContinent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    private static readonly List<string> delayedStates = [];
    private static readonly List<string> delayedCounties = [];
    private static readonly List<string> delayedCities = [];
    private static HashSet<long> UninhabitedCounties = new HashSet<long>();
    private static HashSet<long> UninhabitedStates = new HashSet<long>();
    private static int _step = 0;

    static Program()
    {
        CreationDateAsString = CreationDate.ToString();
        CreationSessionAsString = CreationSession.ToString();
    }

    static void Main(string[] args)
    {
        Log.Console = false;

        foreach (var arg in args)
        {
            if (arg == "-dump")
                _dump = true;
            else if (arg == "-keep")
                _keep = true;
            else if (arg == "-continue")
                _continue = true;
            else if (arg.StartsWith("-cutoff:"))
                ParseCmdLineValue(arg, ref _populationCutoff);
            else if (arg.StartsWith("-creator:"))
                ParseCmdLineValue(arg, ref _creatorId);
            else if (arg.StartsWith("-practice:"))
                ParseCmdLineValue(arg, ref _practiceAreaId);
            else if (arg.StartsWith("-step:"))
                ParseCmdLineValue(arg, ref _step);
            else if (_server == null)
                _server = arg;
            else if (_database == null)
                _database = arg;
            else
            {
                Console.WriteLine("ImportLocationsFromWeb [-keep] [-dump] server database");
                return;
            }
        }

        _server ??= ".";
        _database ??= "AMBenchmark_DB";
        if (_step > 0)
            _continue = true;

        try
        {
            _connection = new AmbDbConnection($"Server={_server};Database={_database};Integrated Security=True;");

            if (!_continue)
                SetUpTempTables();
 
            // Go through the input files and populate the temp tables
            var done = false;
            for (/**/; !done; ++_step)
            {
                Log.WriteLine("Starting STEP " + _step);
                Log.Indent();

                switch (_step)
                {
                    case 0:  ImportContinents(); break;
                    case 1:  SplitAllCountriesFile(); break;
                    case 2:  ImportCountriesFromAllCountriesFile(); break;
                    case 3:  ImportFromAllCountriesPartialFile("ADM1.txt"); break;
                    case 4:  ImportFromAllCountriesPartialFile("ADM2.txt"); break;
                    case 5:  ImportFromAllCountriesPartialFile("ADM3.txt"); break;
                    case 6:  ImportFromAllCountriesPartialFile("ADM4.txt"); break;
                    case 7:  ImportFromAllCountriesPartialFile("ADM5.txt"); break;
                    case 8:  ImportGeographyLocations(); break;
                    case 9:  ImportCities(); break;
                    case 10: ImportAdmin5(); break;
                    //case 11: DeleteUninhabitedAreas(); break;
                    case 11: ExportCountriesToBenchmark(); break;
                    case 12: ImportCountryCodes(); break;
                    // case 13: ImportAliases(); break;
                   default: done = true; break;
                }

                Log.Outdent();
                Log.WriteLine("STEP " + _step + " completed");
            }

            /*
            foreach (var line in delayedStates)
                ProcessStateRecord(line, false);
            foreach (var line in delayedCounties)
                ProcessCountyRecord(line, false);
            foreach (var line in delayedCities)
                ProcessCityRecord(line, false);

            Log.WriteLine("Uninhabited counties: " + UninhabitedCounties.Count);
            Log.WriteLine("Uninhabited states: " + UninhabitedStates.Count);

            if (_dump)
                Dump();
            */
        }
        catch (Exception e)
        {
            Log.Console = true;
            Log.Error(e);
        }
        finally
        {
            if (!_keep && !_continue)
                TearDownTempTables();
            _connection?.Dispose();
        }
        Log.Dispose();
    }

    private static void ParseCmdLineValue(string arg, ref int value)
    {
        var colon = arg.IndexOf(':');
        if (colon >= 0)
        {
            if (int.TryParse(arg.AsSpan(colon+1), out var v))
                value = v;
        }
    }

    private static void ParseCmdLineValue(string arg, ref long value)
    {
        var colon = arg.IndexOf(':');
        if (colon >= 0)
        {
            if (long.TryParse(arg.AsSpan(colon+1), out var v))
                value = v;
        }
    }

    private static void SetUpTempTables()
    {
        TearDownTempTables();

        var cmd = """
                    CREATE TABLE [dbo].[t_TempGeographyImportTable](
                        [GeoNamesId] [bigint] NOT NULL,
                        [BenchmarkId] [bigint] NULL,
                        [Type] [nvarchar](12) NOT NULL,
                        [CountryCode] [nvarchar](12) NOT NULL,
                        [Admin1] [nvarchar](100) NULL,
                        [Admin2] [nvarchar](100) NULL,
                        [Admin3] [nvarchar](100) NULL,
                        [Admin4] [nvarchar](100) NULL,
                        [Admin5] [nvarchar](100) NULL,
                        [Population] [bigint] NOT NULL,
                        [AsciiName] [nvarchar](100) NOT NULL,
                        [Name] [nvarchar](100) NOT NULL,
                        [Combined] [nvarchar](400) NOT NULL,
                        [Mark] [nchar] NULL,
                        [BenchmarkIdUniquenessEnforcer] AS ISNULL([BenchmarkId], [GeoNamesId] * -1),
                        PRIMARY KEY CLUSTERED 
                        (
                            [GeoNamesId] ASC
                        ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
                         CONSTRAINT [TGIT_BenchmarkId] UNIQUE NONCLUSTERED 
                        (
                            [BenchmarkIdUniquenessEnforcer] ASC
                        ) 
                        WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                    ) ON [PRIMARY]
                """;
        _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_CountryCodeIndex] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_Admin1Index] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC, [Admin1] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_Admin2Index] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC, [Admin1] ASC, [Admin2] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_Admin3Index] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC, [Admin1] ASC, [Admin2] ASC, [Admin3] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_Admin4Index] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC, [Admin1] ASC, [Admin2] ASC, [Admin3] ASC, [Admin4] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);

        cmd = """
                CREATE NONCLUSTERED INDEX [TGIT_Admin5Index] ON [dbo].[t_TempGeographyImportTable](
                    [CountryCode] ASC, [Admin1] ASC, [Admin2] ASC, [Admin3] ASC, [Admin4] ASC, [Admin5] ASC
                ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
            """;
         _connection!.ExecuteNonQuery(cmd);
    }

    private static void TearDownTempTables()
    {
        var cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
                  "DROP INDEX [TGIT_Admin1Index] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
              "DROP INDEX [TGIT_Admin2Index] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
              "DROP INDEX [TGIT_Admin3Index] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
              "DROP INDEX [TGIT_Admin4Index] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
                  "DROP INDEX [TGIT_Admin5Index] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
              "DROP INDEX [TGIT_CountryCodeIndex] ON [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);

        cmd = "IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_TempGeographyImportTable]') AND type in (N'U')) " +
              "DROP TABLE [dbo].[t_TempGeographyImportTable]";
        _connection!.ExecuteNonQuery(cmd);
    }

    private static void ImportContinents()
    {
        ProcessLines(".", "CountriesToContinents.txt", (line) =>
        {
            var fields = line.Split('\t', StringSplitOptions.TrimEntries);
            var countryCode = fields[0].Trim();
            if (countryCode != "")
            {
                var continent = fields[2].Trim();
                CountryToContinent.Add(countryCode, continent);
            }
        });
        AddContinentsToDatabase();
        Log.Flush();
    }

    private static void SplitAllCountriesFile()
    {
        const string folder = "allCountries";
        var outputs = new Dictionary<string, StreamWriter>();

        // split the lines across several files
        ProcessLines(folder, "allCountries.txt", (line) =>
        {
            var x = SplitData(line);
            if (x != "")
            {
                //cleaned.WriteLine(line);
                if (!outputs.TryGetValue(x, out var output))
                    outputs.Add(x, output = File.CreateText($"{folder}/{x}.txt"));
                output.WriteLine(line);
            }
        });

        foreach (var output in outputs.Values)
            output.Close();
    }

    private static void ImportCountriesFromAllCountriesFile()
    {
        const string folder = "allCountries";
        ProcessLines(folder, "PCLI.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLD.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "TERR.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLIX.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLS.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLF.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCL.txt", ProcessAllCountriesLine);        
        Log.Flush();
    }

    private static void ImportFromAllCountriesPartialFile(string filename)
    {
        const string folder = "allCountries";
        ProcessLines(folder, filename, ProcessAllCountriesLine);
        Log.Flush();
    }

    private static void ImportGeographyLocations()
    {
        const string folder = "allCountries";
        //using var cleaned = File.CreateText($"/allCountries/processed.txt");
        var outputs = new Dictionary<string, StreamWriter>();

        // split the lines across several files
        ProcessLines(folder, "allCountries.txt", (line) =>
        {
            var x = SplitData(line);
            if (x != "")
            {
                //cleaned.WriteLine(line);
                if (!outputs.TryGetValue(x, out var output))
                    outputs.Add(x, output = File.CreateText($"{folder}/{x}.txt"));
                output.WriteLine(line);
            }
        });

        foreach (var output in outputs.Values)
            output.Close();

        ProcessLines(folder, "PCLI.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLD.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "TERR.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLIX.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLS.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCLF.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "PCL.txt", ProcessAllCountriesLine);        
        ProcessLines(folder, "ADM1.txt", ProcessAllCountriesLine);
        // ProcessLines(allCountries, "ADM1H.txt", line => ProcessStateRecord(line, true));
        ProcessLines(folder, "ADM2.txt", ProcessAllCountriesLine);
        // ProcessLines(allCountries, "ADM2H.txt", line => ProcessCountyRecord(line, true));
        ProcessLines(folder, "ADM3.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "ADM4.txt", ProcessAllCountriesLine);
        ProcessLines(folder, "ADM5.txt", ProcessAllCountriesLine);
        // ProcessLines(folder, "PPL.txt", ProcessAllCountriesLine);
        Log.Flush();
    }

    private static void ImportCities()
    {
        const string folder = "cities500";

        // split the lines across several files
        ProcessLines(folder, "cities500.txt", ProcessAllCountriesLine);
        Log.Flush();
    }

    private static void ProcessAllCountriesLine(string line)
    {
        var fields = line.Split('\t');
        if (fields.Length != 19)
        {
            Log.Error("Bad line in allCountries.txt: " + line);
            return;
        }

        var admin = new string[4] { "NULL", "NULL", "NULL", "NULL" };
        var combined = "N'" + fields[FieldIndex.CountryCode];
        for (var i = 0; i < 4; ++i)
        {
            var a = fields[FieldIndex.Admin1 + i].Trim();
            if (a.Length == 0)
                break;
            a = a.Replace("'", "''");
            admin[i] = "N'" + a + "'";
            combined += "." + a;
        }
        combined += "'";

        var name = fields[FieldIndex.Name].Replace("'", "''");
        var asciiName = fields[FieldIndex.AsciiName].Replace("'", "''");

        var cmd = $"""
                   IF NOT EXISTS (SELECT * FROM [dbo].[t_TempGeographyImportTable] WHERE [GeoNamesId] = {fields[FieldIndex.Id]})
                   INSERT INTO [dbo].[t_TempGeographyImportTable]
                   ([GeoNamesId], [Type], [CountryCode], [Admin1], [Admin2], [Admin3], [Admin4], [Admin5], [Population], [AsciiName], [Name], [Combined], [Mark])
                   VALUES
                   ({fields[FieldIndex.Id]}, '{fields[FieldIndex.FeatureCode]}', '{fields[FieldIndex.CountryCode]}', 
                    {admin[0]}, {admin[1]}, {admin[2]}, {admin[3]}, NULL, {fields[FieldIndex.Population]}, N'{asciiName}', N'{name}', {combined}, NULL)
                """;
        _connection!.ExecuteNonQuery(cmd);
    }

    private static void ImportAdmin5()
    {
        const string folder = "adminCode5";
        var outputs = new Dictionary<string, StreamWriter>();

        // split the lines across several files
        ProcessLines(folder, "adminCode5.txt", (line) =>
        {
            var parts = line.Split('\t', StringSplitOptions.TrimEntries);
            var cmd = $"""
                   UPDATE [dbo].[t_TempGeographyImportTable]
                   SET [Admin5] = N'{parts[1]}', [Combined] = [Combined] + N'.{parts[1]}'
                   WHERE [GeoNamesId] = {parts[0]} AND [Admin3] IS NOT NULL AND [Admin4] IS NOT NULL
                """; 
            var result = _connection!.ExecuteNonQuery(cmd);    
            //if (result != 1)
            //    Log.Error($"Failed to update Admin5 for {parts[0]}");
        });
    }


    private static void ImportCountryCodes()
    {
        // split the lines across several files
        ProcessLines(".", "countryInfo.txt", (line) =>
        {
            var fields = line.Split('\t', StringSplitOptions.TrimEntries);
            var i = 0;
            var iso2 = fields[i++];
            var iso3 = fields[i++];
            var isoNumeric = fields[i++];
            var fips = fields[i++];
            var country = fields[i++];
            var capital = fields[i++];
            var area1 = fields[i++];
            var population = fields[i++];
            var continent = fields[i++];
            var tld = fields[i++];
            var currencyCode = fields[i++];
            var currencyName = fields[i++];
            var phone = fields[i++];
            var postalCodeFormat = fields[i++];
            var postalCodeRegex = fields[i++];
            var languages = fields[i++];
            var geonameid = long.Parse(fields[i++]);
            var neighbours = fields[i++];
            var equivalentFipsCode = fields[i++];

            if (AreasById.TryGetValue(geonameid, out var area))
            {
                AddAliasToDatabase(area.BenchmarkId, iso2, area.Description, 500);
                AddAliasToDatabase(area.BenchmarkId, iso3, area.Description, 500);
                // AddEnglishAliasToDatabase(area, isoNumeric, area.Description);
                AddAliasToDatabase(area.BenchmarkId, country, area.Description, 500);
            }
            else
            {
                Log.Error("ERROR: Unknown country: " + iso2 + " " + country + "    country (" + geonameid + ")");
            }
        });
        Log.Flush();
    }

    private static void DeleteUninhabitedAreas()
    {
        // Mark all areas inhabited
        var cmd = "UPDATE t_TempGeographyImportTable SET Mark = 'I'";
        _connection!.ExecuteNonQuery(cmd);

        // Mark the admin areas as uninhabited
        cmd = "UPDATE t_TempGeographyImportTable SET Mark = 'U' WHERE [Type] LIKE 'ADM%'";
        _connection!.ExecuteNonQuery(cmd);

        // Mark the parents of inhabited areas as inhabited
        while (true)
        {
            cmd = "UPDATE t_TempGeographyImportTable SET Mark = 'I' WHERE [Combined] IN (SELECT [Combined] FROM t_TempGeographyImportTable WHERE [Mark] = 'I')";
            var result = _connection!.ExecuteNonQuery(cmd);
            if (result == 0)
                break;
        }

        // Mark uninhabited areas as dead
        cmd = "UPDATE t_TempGeographyImportTable SET Mark = 'X' WHERE Mark='U'";
        _connection!.ExecuteNonQuery(cmd);
    }

    private static void ExportCountriesToBenchmark()
    {
        var cmd = "SELECT GeoNamesId FROM [t_TempGeographyImportTable] WHERE [BenchmarkId] IS NULL AND [Type] NOT LIKE 'ADM%' AND [Type] NOT LIKE 'PPL%'";
        var gnis = _connection!.ExecuteReader(cmd, (reader) => reader.GetInt64(0));

        foreach (var gni in gnis)
        {
            cmd = $"SELECT CountryCode, Name, AsciiName FROM [t_TempGeographyImportTable] WHERE [GeoNamesId] = {gni}";
            var temp = _connection.SelectOne(cmd, reader => new { GeoNamesId = gni, CountryCode = reader.GetString(0), Name = reader.GetString(1), AsciiName = reader.GetString(2) });

            {
                var cmd = $"""
                    INSERT INTO [dbo].[t_Benchmark]
                    ([CreatorId], [PracticeAreaId], [CreationDate], [CreationSession], [Description], [Type], [Status], [IsDeleted])
                    VALUES
                    ({_creatorId}, {_practiceAreaId}, '{CreationDateAsString}', '{CreationSessionAsString}', N'{area.Name}', 'Country', 'Active', 0)
                """;
                _connection!.ExecuteNonQuery(cmd);

                cmd = $"""
                    UPDATE [dbo].[t_TempGeographyImportTable]
                    SET [BenchmarkId] = (SELECT [Id] FROM [dbo].[t_Benchmark] WHERE [CreatorId] = {_creatorId} AND [PracticeAreaId] = {_practiceAreaId} AND [CreationDate] = '{CreationDateAsString}' AND [CreationSession] = '{CreationSessionAsString}' AND [Description] = N'{area.Name}')
                    WHERE [GeoNamesId] = {area.GeoNamesId}
                """;
                _connection!.ExecuteNonQuery(cmd);
            });
        }

        var cmd = $"""
                    INSERT INTO [dbo].[t_Benchmark]
                    ([CreatorId], [PracticeAreaId], [CreationDate], [CreationSession], [Description], [Type], [Status], [IsDeleted])
                        SELECT DISTINCT {_creat}, {1}, '{2}', '{3}', [Combined], 'Location', 'Active', 0
                        FROM [dbo].[t_TempGeographyImportTable]
                        WHERE [Type] NOT LIKE 'ADM%' AND [Type] NOT LIKE 'PPL%'
                    """;
        cmd = string.Format(cmd, _creatorId, _practiceAreaId, CreationDateAsString, CreationSessionAsString);
        _connection!.ExecuteNonQuery(cmd);

        cmd = """
                    UPDATE [dbo].[t_TempGeographyImportTable]
                    SET [BenchmarkId] = b.[Id]
                    FROM [dbo].[t_TempGeographyImportTable] t
                    JOIN [dbo].[t_Benchmark] b ON b.[CreatorId] = {0} AND b.[PracticeAreaId] = {1} AND b.[CreationDate] = '{2}' AND b.[CreationSession] = '{3}' AND b.[Description] = t.[Combined]
                    WHERE t.[Mark] = 'I'
                """;
        cmd = string.Format(cmd, _creatorId, _practiceAreaId, CreationDateAsString, CreationSessionAsString);
        _connection!.ExecuteNonQuery(cmd);
    }



    /*
    private static void ProcessCountryRecord(string line, bool optional)
    {
        var fields = line.Split('\t');
        var countryCode = fields[FieldIndex.CountryCode].Trim();

        if (Countries.ContainsKey(countryCode))
        {
            if (optional)
                return;
            Log.Error("Duplicate country code: " + countryCode);
        }
        else
        {
            var country = new Country(fields);
            Countries.Add(country.CountryCode, country);
            AreasById.Add(country.Id, country);
            AddCountryToDatabase(country);
        }
    }

    private static void ProcessStateRecord(string line, bool permitDelays)
    {
        var fields = line.Split('\t');

        var state = new Division1(fields);
        if (Countries.TryGetValue(state.CountryCode, out var country))
        {
            
            if (country.States.ContainsKey(state.Admin1))
            {
                Log.Error("ERROR: Duplicate state code: " + state.CountryCode + "." + state.Admin1 + "    state (" + state.Id + ")");
            }
            //else if (state.Population < _populationCutoff)
            //{
            //    Log.Error("State " + state.AsciiName + " [" + state.CountryCode + "." + state.Admin1 + "] has too little population    (" + state.Id + ")");
            //}
            else
            {
                country.States.Add(state.Admin1, state);
                AreasById.Add(state.Id, state);
                AddStateToDatabase(country, state);
                UninhabitedStates.Add(state.Id);
            }
        }
        else
        {
            if (permitDelays)
                delayedStates.Add(line);
            else
                Log.Error("ERROR: Unknown country code: " + state.CountryCode + "    state (" + state.Id + ")");
        }
    }

    private static void ProcessCountyRecord(string line, bool permitDelays)
    {
        var fields = line.Split('\t');

        var county = new Division2(fields);
        if (Countries.TryGetValue(county.CountryCode, out var country))
        {
            if (country.States.TryGetValue(county.Admin1, out var state))
            {
                if (state.Counties.ContainsKey(county.Admin2))
                {
                    Log.Error("ERROR: Duplicate county code: " + county.CountryCode + "." + county.Admin1 + "." + county.Admin2 + "    county (" + county.Id + ")");
                }
                //else if (county.Population < _populationCutoff)
                //{
                //    Log.Error("County " + county.AsciiName + " [" + county.CountryCode + "." + county.Admin1 + "." + county.Admin2 + "] has too little population    (" + county.Id + ")");
                //}
                else
                {
                    state.Counties.Add(county.Admin2, county);
                    AreasById.Add(county.Id, county);
                    AddCountyToDatabase(country, state, county);
                    UninhabitedCounties.Add(county.Id);
                }
            }
            else
            {
                if (permitDelays)
                    delayedCounties.Add(line);
                else                
                    Log.Error("ERROR: Unknown state code: " + county.CountryCode + "." + county.Admin1 + "    county (" + county.Id + ")");
            }
        }
        else
        {
            if (permitDelays)
                delayedCounties.Add(line);
            else            
                Log.Error("ERROR: Unknown country code: " + county.CountryCode + "    county (" + county.Id + ")");
        }
    }

    private static void ProcessCityRecord(string line, bool permitDelays)
    {
        var fields = line.Split('\t');
        var city = new City(fields);
        if ((city.Population > 0) && (city.Population < _populationCutoff))
        {
            Log.Error($"City {city.Id} {city.AsciiName} [{city.CountryCode}.{city.Admin1}.{city.Admin2}] has too little population at {city.Population}");
        }
        else
        {
            if (Countries.TryGetValue(city.CountryCode, out var country))
            {
                if (city.Admin1 == "")
                {
                    country.Cities.Add(city.Id, city);
                    AreasById.Add(city.Id, city);
                    AddCityToDatabase(country, city);
                }
                else if (country.States.TryGetValue(city.Admin1, out var state))
                {
                    if (city.Admin2 == "")
                    {
                        state.Cities.Add(city.Id, city);
                        AreasById.Add(city.Id, city);
                        AddCityToDatabase(state, city);
                        UninhabitedStates.Remove(state.Id);
                    }
                    else if (state.Counties.TryGetValue(city.Admin2, out var county))
                    {
                        county.Cities.Add(city.Id, city);
                        AreasById.Add(city.Id, city);
                        AddCityToDatabase(county, city);
                        UninhabitedCounties.Remove(county.Id);
                        UninhabitedStates.Remove(state.Id);
                    }
                    else
                    {
                        if (permitDelays)
                        {
                            delayedCities.Add(line);
                        }
                        else // if (city.Admin2 == "00")
                        {
                            state.Cities.Add(city.Id, city);
                            AreasById.Add(city.Id, city);
                            AddCityToDatabase(state, city);
                            UninhabitedStates.Remove(state.Id);
                        }
                        //else
                        //{
                        //    Log.Error("ERROR: Unknown county code: " + city.CountryCode + "." + city.Admin1 + "." + city.Admin2 + "    city (" + city.Id + ")");
                        //}
                    }
                }
                else
                {
                    if (permitDelays)
                    {
                        delayedCities.Add(line);
                    }
                    else // if (city.Admin1 == "00")
                    {
                        country.Cities.Add(city.Id, city);
                        AreasById.Add(city.Id, city);
                        AddCityToDatabase(country, city);
                    }
                    //else
                    //{
                    //     Log.Error("ERROR: Unknown state code: " + city.CountryCode + "." + city.Admin1+ "    city (" + city.Id + ")");
                    //}
                }
            }
            else
            {
                if (permitDelays)
                    delayedCities.Add(line);
                else               
                    Log.Error("ERROR: Unknown country code: " + city.CountryCode+ "    city (" + city.Id + ")");
            }
        }
    }
    */

    //private static HashSet<string> _english = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    //private static HashSet<string> _englishLike = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    //private static HashSet<string> _foreign = new HashSet<string>(StringComparer.OrdinalIgnoreCase);


    private static void ImportAliases()
    {
        const string folder = "alternateNamesV2";

        // split the lines across several files
        ProcessLines(folder, "alternateNamesV2.txt", (line) =>
        {
            var fields = line.Split('\t', StringSplitOptions.TrimEntries);

            if ((fields[FieldIndex.IsHistoric] == "1") || (fields[FieldIndex.IsColloquial] == "1"))
                return;
            var language = fields[FieldIndex.Language];
            if (language is "link" or "wkdt" or "post" or "iata" or "icao" or "faac" or "fr_1793" or "unlc")
                return;

            var alias = fields[FieldIndex.AlternatName];
            var isEnglish = false;
            if ((language == "") || language.StartsWith("en-")) 
            {
                isEnglish = TryTranslate(alias, out var translated);
                if (!isEnglish)
                {
                    Log.WriteLine("Could not translate: " + alias);
                    return;
                }
                alias = translated;
            }

            var areaId = long.Parse(fields[FieldIndex.GeographicId]);
            if (!AreasById.TryGetValue(areaId, out var area))
                return;

            // bool isEnglish = IsEnglish(alias);
            if (isEnglish)
            {
                //_english.Add(language);
                AddAliasToDatabase(area!.BenchmarkId, alias!, area.Description, 500);
            }
            else
            {
                //_foreign.Add(language);
            }
        });
    }

    private static string? ToEnglish(string name)
    {
        var n = name.Normalize(System.Text.NormalizationForm.FormD);
        var a = n.ToCharArray()
            .Where(c => (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark) && (c < 0x007F))
            .ToArray();
        if (a.Length == 0)
            return null;
        return new string(a);
    }

    private static bool TryTranslate(string name, out string? translation)
    {
        translation = ToEnglish(name);
        return translation != null;
    }

    private static string SplitData(string line)
    {
        var fields = line.Split('\t', StringSplitOptions.TrimEntries);
        if (fields.Length != 19)
        {
            Log.Error($"Expected 19 fields, saw {fields.Length}");
            return "";
        }

        var featureClass = fields[FieldIndex.FeatureClass].Trim();            //    : see http://www.geonames.org/export/codes.html, char(1)
        if ((featureClass != "A") && (featureClass != "P") && featureClass != "")
            return "";
        var featureCode = fields[FieldIndex.FeatureCode].Trim();           // feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
        return (featureCode == "") ? "X" : featureCode.ToUpper();
    }

    /*
    private static bool DownloadFile(string url)
    {
        var slash = url.LastIndexOf('/');
        var filename = url.Substring(slash + 1);
        if (_quick && File.Exists(filename))
            return true;
        try
        {
            using var client = new HttpClient();
            using var s = client.GetStreamAsync(url);
            using var fs = new FileStream(filename, FileMode.OpenOrCreate);
            s.Result.CopyTo(fs);
            Log.WriteLine($"Downloaded file: {filename} from {url}");
            return true;
        }
        catch (Exception ex)
        {
            Log.Error($"Exception while downloading file: {filename} from {url}");
            Log.Error(ex);
            return false;
        }
    }

    private static bool UnzipFile(string zipFilename, string? directory = null)
    {
        var name = Path.GetFileNameWithoutExtension(zipFilename);
        try
        {
            directory ??= name;
            if (_quick)
            {
                if (Directory.Exists(directory) && File.Exists(Path.Combine(directory, name + ".txt")))
                    return true;
            }
            else
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, true);
                Directory.CreateDirectory(directory);
            }
            System.IO.Compression.ZipFile.ExtractToDirectory(zipFilename, directory + "\\");
            Log.WriteLine($"Unzipped file: {zipFilename}");
            return true;
        }
        catch (Exception ex)
        {
            Log.Error($"Exception while unzipping file: {zipFilename}");
            Log.Error(ex);
            return false;
        }
    }
    */

    private static void ProcessLines(string folder, string filename, Action<string> action)
    {
        try
        {   
            var path = Path.GetFullPath(folder);
            var files = Directory.GetFiles(path, filename);
            foreach (var file in files)
            {
                Log.WriteLine($"Processing file {filename}");
                Log.Indent();

                int line = 1;
                using var f = File.OpenText(file);
                while (true)
                {
                    var line2 = f.ReadLine();
                    if (line2 == null)
                        break;
                    //Log.WriteLine($"Processing line {line}").Indent();
                    ++line;
                    if (line2.StartsWith("#"))
                        continue;
                    if ((line > 0) && (line % 1000 == 0))
                        Log.WriteLine($"Processing line {line}");
                    try
                    {
                        action(line2);
                    }
                    catch (Exception e)
                    {
                        Log.Error($"Exception while processing line {line}");
                        Log.Error(e);
                        //Log.Outdent();
                        if (Debugger.IsAttached)
                            Debugger.Break();
                    }
                    finally
                    {
                        //Log.Outdent();
                    }
                }

                Log.Outdent();
                Log.WriteLine($"End of {filename} after {line} lines");

                //Log.WriteLine("Excluded feature codes: ").Indent();
                //var efc = ExcludedFeatureCodes.ToList();
                //efc.Sort();
                //foreach (var x in efc)
                //    Log.WriteLine(x);
                //Log.Outdent();
            }
        }
        catch (Exception ex)
        {
            Log.Outdent();
            Log.Error($"Exception while processing file: {filename}");
            Log.Error(ex);
        }
    }


    private static long _worldId = 0;
    private static Dictionary<string, long> _continents = new (); // benchmark name -> benchmark id
    private static string _inContinents = "";

    private static void AddContinentsToDatabase()
    {
        var w = _connection!.SelectOneValue("SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] IS NULL", (r) => r.GetInt64(0));
        if (!w.HasValue)
            throw new Exception("World not found");
        _worldId = w.Value;
        _continents = _connection.Select($"SELECT [OID], [NAME] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {_worldId}", (r) => new Tuple<string, long>(r.GetString(1), r.GetInt64(0)));
        var ctc = new HashSet<string>(CountryToContinent.Values, StringComparer.OrdinalIgnoreCase);

        foreach (var continent in ctc)
        {
            if (!_continents.ContainsKey(continent))
            {
                var index = NextChildIndex(_worldId);
                var oid = _connection.GetNextOid();
                _connection!.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                    "([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorId],[PracticeAreaID],[CreationSession]) " +
                    "VALUES " +
                    $"({oid}, {_worldId}, 1,'{continent}', {index}, '{continent}', '{CreationDateAsString}', {_creatorId}, {_practiceAreaId}, '{CreationSessionAsString}')");
                _continents.Add(continent, oid);
                AddAliasToDatabase(oid, continent, continent, 500);
            }
            else
            {
                AddAliasToDatabase(_continents[continent], continent, continent, 500);
            }
        }

        _inContinents  = "[PID] IN (" + string.Join(", ", _continents.Values) + ")";
    }

    private static void AddCountryToDatabase(Country entry)
    {
        var name = entry.AsciiName.Replace("'", "''");
        var lname = name.ToLower();
        var c = _connection!.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE LOWER([NAME]) = '{lname}' AND {_inContinents}", (r) => r.GetInt64(0));
        if (!c.HasValue)
        {
            c = _connection!.SelectOneValue("SELECT L.[OID] FROM [dbo].[t_GeographicLocationAlias] A " + 
                                            "JOIN [dbo].[t_GeographicLocation] L ON (L.[OID] = A.[GeographicLocationID]) " +
                                           $"WHERE LOWER(A.[Alias]) = '{lname}' AND L.{_inContinents}", 
                                           (r) => r.GetInt64(0));
            if (!c.HasValue)
            {
                c = _connection!.SelectOneValue("SELECT L.[OID] FROM [dbo].[t_GeographicLocationAlias] A " + 
                                                "JOIN [dbo].[t_GeographicLocation] L ON (L.[OID] = A.[GeographicLocationID]) " +
                                               $"WHERE LOWER(A.[Alias]) = '{entry.CountryCode.ToLower()}' AND L.{_inContinents}", 
                                           (r) => r.GetInt64(0));
                if (!c.HasValue)
                {
                    if (CountryToContinent.TryGetValue(entry.CountryCode, out var continentName))
                    {
                        var continentId = _continents[continentName];
                        AddChildGeographicLocationToDatabase(continentId, entry);
                        AddAliasToDatabase(entry.BenchmarkId, entry.CountryCode, entry.AsciiName, 500);
                        return;
                    }
                    else
                    {
                        throw new Exception($"Country {entry.AsciiName} not found");
                    }
                }
                else
                {
                    entry.BenchmarkId = c.Value;
                }
            }
            else
            {
                entry.BenchmarkId = c.Value;
            }
        }
        else
        {
            entry.BenchmarkId = c.Value;
        }

        AddAliasToDatabase(entry.BenchmarkId, entry.AsciiName, entry.AsciiName, 500);
        AddAliasToDatabase(entry.BenchmarkId, entry.CountryCode, entry.AsciiName, 500);
        if ((entry.Name != entry.AsciiName) && TryTranslate(entry.Name, out var translation))
            if (translation != entry.AsciiName)
                AddAliasToDatabase(entry.BenchmarkId, translation!, entry.AsciiName, 500);
    }


    static readonly Dictionary<long, long> _previousChildIndex = new();

    private static long NextChildIndex(long parentId)
    {
        long nextIndex;
        if (_previousChildIndex.ContainsKey(parentId))
        {
            nextIndex = _previousChildIndex[parentId] + 1;
        }
        else
        {
            var i = _connection!.ExecuteScalar($"SELECT MAX([Index]) FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parentId}");
            if (i is int j)
            {
                nextIndex = j;
                ++nextIndex;
            }
            else if (i is long l)
            {
                nextIndex = l;
                ++nextIndex;
            }
            else if (i is DBNull)
            {
                nextIndex = 0;
            }
            else
            {
                throw new Exception($"Could not get index");
            }
        }

        _previousChildIndex[parentId] = nextIndex;
        return nextIndex; 
    }


    private static string[] EndingRedundancies = new string[] { " Region", " District", " Province", " County", " Parish" }; // need to have a space before each one

    private static void AddStateToDatabase(Country country, Division1 state)
    {
        AddChildGeographicLocationToDatabase(country, state);
    }

    private static void AddCountyToDatabase(Country country, Division1 state, Division2 county)
    {
        AddChildGeographicLocationToDatabase(state, county);
    }

    private static void AddCityToDatabase(Entry parent, City city)
    {
        AddChildGeographicLocationToDatabase(parent, city);
    }

    private static void AddChildGeographicLocationToDatabase(Entry parent, Entry child)
        => AddChildGeographicLocationToDatabase(parent.BenchmarkId, child);

    private static void AddChildGeographicLocationToDatabase(long parentId, Entry child)
    {
        if (parentId == 0)
            throw new Exception($"Parent of {child.AsciiName} not found");

        // We use the GeoNames AsciiName as the GL's name, and as the first (primary) alias
        var name = child.AsciiName.Replace("'", "''");
        var lname = name.ToLower();
        var c = _connection!.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE LOWER([NAME]) = '{lname}' AND [PID] = {parentId}", (r) => r.GetInt64(0));
        if (!c.HasValue)
        {
            var index = NextChildIndex(parentId);
            child.BenchmarkId = _connection.GetNextOid();

            _connection!.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                "([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorId],[PracticeAreaID],[CreationSession]) " +
                "VALUES " +
                $"({child.BenchmarkId}, {parentId}, 1,'{name}', {index}, '{name}', '{CreationDateAsString}', {_creatorId}, {_practiceAreaId}, '{CreationSessionAsString}')");
        }
        else
        {
            child.BenchmarkId = c.Value;
        }    

        // We use the GeoNames AsciiName as the GL's name, and as the first (primary) alias
        AddAlias(child.AsciiName);

        if ((child.Name != child.AsciiName) && TryTranslate(child.Name, out var translation))
            if (translation != child.AsciiName)
                AddAlias(translation!);

        return;

        void AddAlias(string a)
        {
            AddAliasToDatabase(child.BenchmarkId, a, child.AsciiName, 500);
            foreach (var ending in EndingRedundancies)
            {
                if (a.EndsWith(ending))
                {
                    AddAliasToDatabase(child.BenchmarkId, a.Substring(0, a.Length - ending.Length), child.AsciiName, 500);
                    break;
                }
            }
        }
    }

    private static long memoBenchmarkId;
    private static long memoLanguageId;
    private static string memoAlias = "";

    private static void AddAliasToDatabase(long benchmarkId, string alias, string description, long languageId)
    {
        // There are a LOT of duplicates, so we can skip them
        if ((benchmarkId == memoBenchmarkId) && (languageId == memoLanguageId) && (alias == memoAlias))
            return;
        memoBenchmarkId = benchmarkId;
        memoLanguageId = languageId;
        memoAlias = alias;

        var name = alias.Replace("'", "''");
        var lname = name.ToLower();
        var c = _connection!.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE LOWER([Alias]) = '{lname}' AND [GeographicLocationId] = {benchmarkId}", (r) => r.GetInt64(0));
        if (c.HasValue)
            return;

        var createAsPrimary = (languageId == 500);
        var c2 = _connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [IsPrimary] = 1 AND [GeographicLocationId] = {benchmarkId}", (r) => r.GetInt64(0));
        if (c2.HasValue)
            createAsPrimary = false;
        var p = createAsPrimary ? 1 : 0;

        var oid = _connection.GetNextOid();
        description = description.Replace("'", "''");
        _connection!.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocationAlias] " +
            "([OID],[Alias],[Description],[IsSystemOwned],[IsPrimary],[CreationDate],[CreatorId],[PracticeAreaID],[GeographicLocationID],[LID],[CreationSession]) " +
            "VALUES " +
            $"({oid}, '{name}', '{description}', 1, {p}, '{CreationDateAsString}', {_creatorId}, {_practiceAreaId}, {benchmarkId}, {languageId},'{CreationSessionAsString}')");
    }

    private static void Dump()
    {
        using var file = File.CreateText("Dump.txt");
        Dump(20000, 0, file);
    }

    private static void Dump(long benchmarkId, int indentation, StreamWriter writer)
    {
        var oids = _connection!.Select($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {benchmarkId}", (r) => r.GetInt64(0));
        foreach (var oid in oids)
        {
            var names = _connection.Select($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid} ORDER BY IsPrimary DESC, Alias", (r) => r.GetString(0));
            
            for (var i=0; i<indentation; i++)
                writer.Write("\t");

            writer.Write(oid);
            writer.Write(": ");
            writer.Write(names[0]);

            for (var i=1; i<names.Count; i++)
            {
                writer.Write(", ");
                writer.Write(names[i]);
            }

            writer.WriteLine();
            Dump(oid, indentation + 1, writer);
        }
   }

}
