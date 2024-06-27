using System.Data;
using AmbHelper;
using System.Globalization;
using static AmbHelper.Logs;

namespace ImportGeographicLocationsFromGeoNames;

internal partial class Program
{
    /*
    private static readonly Dictionary<string, Country> Countries = new (StringComparer.OrdinalIgnoreCase);
    private static readonly Dictionary<long, Entry> AreasById = new ();
    */
    private string _server = ".";
    private string _database = "AMBenchmark_DB";
    private int _line = 0;
    private AmbDbConnection? _connection;
    private AmbDbConnection Connection => _connection!;
    static DateTime MinDateTime = new DateTime(1800, 1, 1);
    static DateTime MaxDateTime = new DateTime(2999, 1, 1);
   /*
    private static bool _quick = false;
    private static bool _dump = false;
    */
    private readonly DateTime _creationDate;
    private readonly string _creationDateAsString;
    private readonly Guid _creationSession = Guid.NewGuid();
    private readonly string _creationSessionAsString;
    private long _creatorId = 100;
    private int _practiceAreaId = 2501;
    private int _step = 0;
    private bool _keep = false;
    /*
    // private static long _populationCutoff = 1000;
    private static Dictionary<string, string> CountryToContinent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    private static readonly List<string> delayedStates = [];
    private static readonly List<string> delayedCounties = [];
    private static readonly List<string> delayedCities = [];
    private static HashSet<long> UninhabitedCounties = new HashSet<long>();
    private static HashSet<long> UninhabitedStates = new HashSet<long>();
    */
    private readonly HashSet<long> _geoNameIds = new();
    private long _worldId = 20000;
    private readonly Dictionary<string, string> _continentNameToAbbreviation;
    private readonly Dictionary<string, long> _continentAbbreviationToId = new (StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _geoNameCountryCodesToIds = new (StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<long, long> _geoNameCountryIdsToBenchmarkIds= new ();
    private readonly Dictionary<long, long> _previousChildIndex = new ();


    static void Main(string[] args)
    {
        var program = new Program();
        try
        {
            program.Run(args);
        }
        catch (Exception e)
        {
            Error.WriteLine(e);
        }
        finally
        {
            Logs.Dispose();
        }
    }

    private Program()
    {
        _creationDate = DateTime.Now;
        _creationDateAsString = _creationDate.ToString(CultureInfo.InvariantCulture);
        _creationSession = Guid.NewGuid();
        _creationSessionAsString = _creationSession.ToString();

        _continentNameToAbbreviation = new(StringComparer.OrdinalIgnoreCase)
        {
            { "Europe", "EU" },
            { "Middle East", "ME" },
            { "Central Asia", "CA" },
            { "North America", "NA" },
            { "Latin America", "LA" },
            { "Africa", "AF" },
            { "Antarctica", "AN" },
            { "North Asia", "NO" },
            { "Southeast Asia & Australia (SEAA)", "SA" }
        };

        _continentAbbreviationToId = new (StringComparer.OrdinalIgnoreCase);
    }

    private void Run(string[] args)
    {
        Log.Console = false;

        for (var i=0; i<args.Length; i++)
        {
            var arg = args[i];

            if (arg.StartsWith("-creator"))
                _creatorId = long.Parse(args[++i]);
            else if (arg.StartsWith("-practice"))
                _practiceAreaId = int.Parse(args[++i]);
            else if (arg.StartsWith("-server"))
                _server = args[++i];
            else if (arg.StartsWith("-database"))
                _database = args[++i];
            else if (arg.StartsWith("-step"))
                _step = int.Parse(args[++i]);
            else if (arg.StartsWith("-line"))
                _line = int.Parse(args[++i]);
           else if (_server == null)
                _server = arg;
            else if (_database == null)
                _database = arg;
            else if (arg.StartsWith("-keep"))
                _keep = true;
            //if (arg == "-quick")
            //    _quick = true;
            //else if (arg == "-dump")
            //    _dump = true;
            // else if (arg.StartsWith("-cutoff:"))
            //    ParseCmdLineValue(arg, ref _populationCutoff);
            //else if (arg.StartsWith("-cutoff:"))
            //    ParseCmdLineValue(arg, ref _step);
            else
            {
                Console.WriteLine("Bad syntax");
                return;
            }
        }

        if (_step > 0)
            _keep = true;

        _connection = new AmbDbConnection($"Server={_server};Database={_database};Integrated Security=True;");

        var done = false;
        for (/**/; !done; ++_step)
        {
            Log.WriteLine("STEP " + _step).Flush();
            Log.Indent();

            switch (_step)
            {
                /* GeoNames Import */
                case 0:
                    DownloadFiles();
                    break;

                case 1:
                    ImportFromAllCountriesFile1();
                    break;

                case 2:
                    // ImportFromCitiesFile();
                    break;

               case 3:
                    ImportFromCountryInfoFile();
                    break;

               case 4:
                    ImportCountriesToContinentsFile();
                    break;

               case 5:
                    ImportAlternateNamesFile();
                    break;

                /* Benchmark Import */
                case 6: 
                    ImportContinents(); 
                    break;

                case 7: 
                    ImportCountries(); 
                    break;

                case 8: 
                    ImportStates(); 
                    break;

                case 9: 
                    ImportCounties(); 
                    break;

                case 10: 
                    //ImportCities(); 
                    break;

                /*
                case 1: ImportGeographyLocations(); break;
                case 2: ImportAliases(); break;
                case 3: ImportCountryCodes(); break;
                case 4: ImportCities(); break;

                case 5:
                    foreach (var line in delayedStates)
                        ProcessStateRecord(line, false);
                    foreach (var line in delayedCounties)
                        ProcessCountyRecord(line, false);
                    foreach (var line in delayedCities)
                        ProcessCityRecord(line, false);
                    break;
                */

                default:
                    done = true; 
                    break;
            }

            Log.Outdent();
            Log.Flush();
        }

        /*
//            Log.WriteLine("Uninhabited counties: " + UninhabitedCounties.Count);
//            Log.WriteLine("Uninhabited states: " + UninhabitedStates.Count);

            if (_dump)
                Dump();
        */
    }


    private static DateTime ParseDateTime(string text, DateTime defaultValue)
    {
        try
            {
            if (text == "")
                return defaultValue;

            if (DateTime.TryParse(text, out var result))
                return result;

            if (int.TryParse(text, out var number))
            {
                if (number < 9999)
                    return new DateTime(Math.Max(number, MinDateTime.Year), 1, 1);
                if (number < 999999)
                    return new DateTime(number / 100, number % 100, 1);
                var year = number / 10000;
                var dm = number % 10000;
                return new DateTime(year, (dm / 100), dm % 100);
            }
        }
        catch 
        { 
        }

        return defaultValue;
    }

    /*
    #region ParseCmdLine

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

    #endregion
    */

    #region Import Continents

    private void ImportContinents()
    {
        // Get the world
        var w = Connection.SelectOneValue("SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] IS NULL", (r) => r.GetInt64(0));
        if (!w.HasValue)
        {
            _worldId = 20000;
            Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                "([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorId],[PracticeAreaID],[CreationSession]) " +
                "VALUES " +
                $"({_worldId}, NULL, 1, N'World', 0, N'World', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')");
            AddAliasToDatabase(_worldId, "World");
        }
        else
        {
            _worldId = w.Value;
        }

        // Create any continents that don't already exist
        LoadExistingContinentIds();
        foreach (var kvp in _continentNameToAbbreviation)
        {
            if (_continentAbbreviationToId.ContainsKey(kvp.Value))
                continue;
            var index = NextChildIndex(_worldId);
            var id = Connection.GetNextOid();
            Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                "([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorId], [PracticeAreaID], [CreationSession]) " +
                "VALUES " +
                $"({id}, {_worldId}, 1, N'{kvp.Key}', {index}, N'{kvp.Key}', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')");
            _continentAbbreviationToId.Add(kvp.Value, id);
            AddAliasToDatabase(id, kvp.Key, kvp.Key);
        }
    }
    
    private void LoadExistingContinentIds()
    {
        if (_continentAbbreviationToId.Count > 0)
            return;
        foreach (var kvp in _continentNameToAbbreviation)
        {
            var id = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {_worldId} AND [Name] = N'{kvp.Key}'", (r) => r.GetInt64(0));
            if (id.HasValue)
            {
                _continentAbbreviationToId.Add(kvp.Value, id.Value);
                AddAliasToDatabase(id.Value, kvp.Key, kvp.Key);
            }
        }
    }

    #endregion

    #region Import Countries

    private void ImportCountries()
    {
        LoadExistingContinentIds();
        /*
        // First, read the existing ones from Benchmark
        var countryIds = Connection.Select(
            $"""
                SELECT A.[OID], A.[Name]
                FROM [dbo].[t_GeographicLocation] A 
                WHERE [PID] IN (SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {_worldId})
            """, (r) => new { Oid=r.GetInt64(0), Name=r.GetString(1) });

        // For each of those countries, the name must be in the GeoNames tables.
        // Use this to set the benchmark ID in the GeoNames table.
        foreach (var cid in countryIds)
        {
            var name = cid.Name;
            var geonameId = Connection.SelectOneValue(
                $"""
                    SELECT [GeoNameId]
                    FROM [dbo].[t_GeoNamesAlternateNames]
                    WHERE [AlternateName] = N'{cid.Name}'
                 """, (r) => r.GetInt64(0));

            if (!geonameId.HasValue)
                throw new Exception("Country not found in GeoNames: " + name);
            Connection.ExecuteNonQuery(
                $"""
                    UPDATE [dbo].[t_GeoNames]
                        SET [BenchmarkId] = {cid.Oid}
                        WHERE [GeoNameId] = {geonameId}

                 """);
        }
        */

        // Now, process those countries not already in Benchmark
        ImportFlatTextFile("countryInfo.txt", '\t', 19, (lineNumber, fields) =>
        {
            var i = 0;
            var iso = fields[i++];
            var iso3 = fields[i++];
            var isoNumeric = fields[i++];
            var fips = fields[i++];
            var country = fields[i++];
            var capital = fields[i++];
            var area = fields[i++];
            var population = fields[i++];
            var continent = fields[i++];
            var tld = fields[i++];
            var currencyCode = fields[i++];
            var currencyName = fields[i++];
            var phone = fields[i++];
            var postalCodeFormat = fields[i++];
            var postalCodeRegex = fields[i++];
            var languages = fields[i++];
            var geonameId = long.Parse(fields[i++]);
            var neighbours = fields[i++];
            var equivalentFipsCode = fields[i++];

            var c = Connection.SelectOne(
                $"""
                    SELECT [Continent]
                    FROM [dbo].[t_GeoNames]
                    WHERE [GeoNameId] = {geonameId}
                 """, (r) => r.GetString(0));

            if (string.IsNullOrWhiteSpace(c))
                throw new Exception($"Country's continent not found in GeoNames: {geonameId}: {country}");

            var parentId = _continentAbbreviationToId[c!];
            var oid = AddGeographicLocationToDatabase(parentId, country);
            AddAliasToDatabase(oid, iso, country);
            AddAliasToDatabase(oid, iso3, country);
            _geoNameCountryCodesToIds.Add(iso, geonameId);
            _geoNameCountryIdsToBenchmarkIds.Add(geonameId, oid);
        });
    }

    #endregion

    #region Import States

    private void ImportStates()
    {
        LoadGeoNameCountries();
 
        foreach (var kvp in _geoNameCountryCodesToIds)
        {
            var names = Connection.Select(
                $"""
                    SELECT [Name]
                    FROM [dbo].[t_GeoNames]
                    WHERE [FeatureCode] = N'ADM1' AND [CountryCode] = N'{kvp.Key}'
                 """, (r) => r.GetString(0));

            var parentId = _geoNameCountryIdsToBenchmarkIds[kvp.Value];
            foreach (var name in names)
            {
                AddGeographicLocationToDatabase(parentId, name);
            }
        }
    }

    #endregion

    private void ImportCounties()
    {
        LoadGeoNameCountries();
 
        foreach (var kvp in _geoNameCountryCodesToIds)
        {
            var names = Connection.Select(
                $"""
                    SELECT G.[Name], G.[NAME]
                    FROM [dbo].[t_GeoNames] G
                    JOIN [dbo].[t_GeoNames] H A ON G.[GeoNameId] = A.[GeoNameId]
                    WHERE [FeatureCode] = N'ADM2' AND [CountryCode] = N'{kvp.Key}'
                 """, (r) => r.GetString(0));

            var parentId = _geoNameCountryIdsToBenchmarkIds[kvp.Value];
            foreach (var name in names)
            {
                AddGeographicLocationToDatabase(parentId, name);
            }
        }
    }

  

    private long AddGeographicLocationToDatabase(long parentId, string asciiName)
    {
        var name = asciiName.Replace("'", "''");
        var oid = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parentId} AND [Name] = N'{name}'",
            (r) => r.GetInt64(0), false);
        if (oid.HasValue)
            return oid.Value;

        var index = NextChildIndex(parentId);
        oid = Connection.GetNextOid();

        Connection.ExecuteNonQuery(
            $"""
                INSERT INTO [dbo].[t_GeographicLocation]
                    ([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorId], [PracticeAreaID], [CreationSession])
                VALUES
                    ({oid.Value}, {parentId}, 1, N'{name}', {index}, N'{name}', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')
             """, false);   

        // We use the GeoNames AsciiName as the GL's name, and as the first (primary) alias
        AddAliasToDatabase(oid.Value, asciiName);
        return oid.Value;
    }


   /*
    private static void ImportCities()
    {
        const string cities = "cities500";
        DownloadFile("https://download.geonames.org/export/dump/" + cities + ".zip");
        UnzipFile(cities + ".zip");

        // split the lines across several files
        ProcessLines(cities, cities + ".txt", (line) =>
        {
            ProcessCityRecord(line, true);
        });

        Log.Flush();
    }


    private static void ImportCountryCodes()
    {
        const string countryInfo = "countryInfo";
        DownloadFile("https://download.geonames.org/export/dump/" + countryInfo + ".txt");

        // split the lines across several files
        ProcessLines(".", countryInfo + ".txt", (line) =>
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
                Error.WriteLine("ERROR: Unknown country: " + iso2 + " " + country + "    country (" + geonameid + ")");
            }
        });
        Log.Flush();
    }


    private static void ProcessCountryRecord(string line, bool optional)
    {
        var fields = line.Split('\t');
        var countryCode = fields[FieldIndex.CountryCode].Trim();

        if (Countries.ContainsKey(countryCode))
        {
            if (optional)
                return;
            Error.WriteLine("Duplicate country code: " + countryCode);
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
                Error.WriteLine("ERROR: Duplicate state code: " + state.CountryCode + "." + state.Admin1 + "    state (" + state.Id + ")");
            }
            //else if (state.Population < _populationCutoff)
            //{
            //    Error.WriteLine("State " + state.AsciiName + " [" + state.CountryCode + "." + state.Admin1 + "] has too little population    (" + state.Id + ")");
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
                Error.WriteLine("ERROR: Unknown country code: " + state.CountryCode + "    state (" + state.Id + ")");
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
                    Error.WriteLine("ERROR: Duplicate county code: " + county.CountryCode + "." + county.Admin1 + "." + county.Admin2 + "    county (" + county.Id + ")");
                }
                //else if (county.Population < _populationCutoff)
                //{
                //    Error.WriteLine("County " + county.AsciiName + " [" + county.CountryCode + "." + county.Admin1 + "." + county.Admin2 + "] has too little population    (" + county.Id + ")");
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
                    Error.WriteLine("ERROR: Unknown state code: " + county.CountryCode + "." + county.Admin1 + "    county (" + county.Id + ")");
            }
        }
        else
        {
            if (permitDelays)
                delayedCounties.Add(line);
            else            
                Error.WriteLine("ERROR: Unknown country code: " + county.CountryCode + "    county (" + county.Id + ")");
        }
    }

    private static void ProcessCityRecord(string line, bool permitDelays)
    {
        var fields = line.Split('\t');
        var city = new City(fields);
        // if ((city.Population > 0) && (city.Population < _populationCutoff))
        // {
        //    Error.WriteLine($"City {city.Id} {city.AsciiName} [{city.CountryCode}.{city.Admin1}.{city.Admin2}] has too little population at {city.Population}");
        // }
        // else
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
                        //    Error.WriteLine("ERROR: Unknown county code: " + city.CountryCode + "." + city.Admin1 + "." + city.Admin2 + "    city (" + city.Id + ")");
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
                    //     Error.WriteLine("ERROR: Unknown state code: " + city.CountryCode + "." + city.Admin1+ "    city (" + city.Id + ")");
                    //}
                }
            }
            else
            {
                if (permitDelays)
                    delayedCities.Add(line);
                else               
                    Error.WriteLine("ERROR: Unknown country code: " + city.CountryCode+ "    city (" + city.Id + ")");
            }
        }
    }

    //private static HashSet<string> _english = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    //private static HashSet<string> _englishLike = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    //private static HashSet<string> _foreign = new HashSet<string>(StringComparer.OrdinalIgnoreCase);


    private static void ImportAliases()
    {
        const string allAliases = "alternateNamesV2";
        DownloadFile("https://download.geonames.org/export/dump/" + allAliases + ".zip");
        UnzipFile(allAliases + ".zip");

        using var cleaned = File.CreateText($"{allAliases}/processed.txt");

        // split the lines across several files
        ProcessLines(allAliases, allAliases + ".txt", (line) =>
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
                    Log.WriteLine("Could not translate [1]: " + alias);
                    return;
                }
                alias = translated;
            }
            else
            {
                isEnglish = TryTranslate(alias, out var translated);
                if (!isEnglish)
                {
                    Log.WriteLine("Could not translate[2]: " + alias);
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
    */

    /*


    private static long _worldId = 0;
    private static Dictionary<string, long> _continents = new (); // benchmark name -> benchmark id
    private static string _inContenents = "";

    private static void AddContinentsToDatabase()
    {
    }

    private static void AddCountryToDatabase(Country entry)
    {
        var name = entry.AsciiName.Replace("'", "''");
        var lname = name.ToLower();
        var c = _connection!.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE LOWER([NAME]) = '{lname}' AND {_inContenents}", (r) => r.GetInt64(0));
        if (!c.HasValue)
        {
            c = _connection!.SelectOneValue("SELECT L.[OID] FROM [dbo].[t_GeographicLocationAlias] A " + 
                                            "JOIN [dbo].[t_GeographicLocation] L ON (L.[OID] = A.[GeographicLocationID]) " +
                                           $"WHERE LOWER(A.[Alias]) = '{lname}' AND L.{_inContenents}", 
                                           (r) => r.GetInt64(0));
            if (!c.HasValue)
            {
                c = _connection!.SelectOneValue("SELECT L.[OID] FROM [dbo].[t_GeographicLocationAlias] A " + 
                                                "JOIN [dbo].[t_GeographicLocation] L ON (L.[OID] = A.[GeographicLocationID]) " +
                                               $"WHERE LOWER(A.[Alias]) = '{entry.CountryCode.ToLower()}' AND L.{_inContenents}", 
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

    */


    private long NextChildIndex(long parentId)
    {
        long nextIndex;
        if (_previousChildIndex.ContainsKey(parentId))
        {
            nextIndex = _previousChildIndex[parentId] + 1;
        }
        else
        {
            var i = Connection.ExecuteScalar($"SELECT MAX([Index]) FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parentId}", false);
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

    /*
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
    */

    //private static long memoBenchmarkId;
    //private static long memoLanguageId;
    //private static string memoAlias = "";

    private void AddAliasToDatabase(long benchmarkId, string alias, string? description = null, long languageId = 500)
    {
        description ??= alias;
        // There are a LOT of duplicates, so we can skip them
        //if ((benchmarkId == memoBenchmarkId) && (languageId == memoLanguageId) && (alias == memoAlias))
        //    return;
        //memoBenchmarkId = benchmarkId;
        //memoLanguageId = languageId;
        //memoAlias = alias;

        var name = alias.Replace("'", "''");
        var lname = name.ToLower();
        var c = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE LOWER([Alias]) = '{lname}' AND [GeographicLocationId] = {benchmarkId}", (r) => r.GetInt64(0));
        if (c.HasValue)
            return;

        var createAsPrimary = (languageId == 500);
        var c2 = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [IsPrimary] = 1 AND [GeographicLocationId] = {benchmarkId}", (r) => r.GetInt64(0));
        if (c2.HasValue)
            createAsPrimary = false;
        var p = createAsPrimary ? 1 : 0;

        var oid = Connection.GetNextOid();
        description = description.Replace("'", "''");
        Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocationAlias] " +
            "([OID],[Alias],[Description],[IsSystemOwned],[IsPrimary],[CreationDate],[CreatorId],[PracticeAreaID],[GeographicLocationID],[LID],[CreationSession]) " +
            "VALUES " +
            $"({oid}, '{name}', '{description}', 1, {p}, '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, {benchmarkId}, {languageId},'{_creationSessionAsString}')");
    }

    /*
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

*/
}