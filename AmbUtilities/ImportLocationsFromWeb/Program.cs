using System.Diagnostics;
using System.Data;
using AmbHelper;
using System.Globalization;


namespace ImportLocations;

internal class Program
{
    private static class FieldIndex
    {
        public const int Id = 0;
        public const int Name = 1;
        public const int AsciiName = 2;
        //i++;                                          // var alternatenames = fields[i++].Split(',');    // alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
        //i++;                                          // latitude: latitude in decimal degrees(wgs84)
        //i++;                                          // longitude: longitude in decimal degrees(wgs84)
        public const int FeatureClass = 6;              
        public const int FeatureCode = 7;
        public const int CountryCode = 8;               // ISO-3166 2-letter country code, 2 characters
        // var cc2 = fields[i++].Trim();                   // alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
        public const int Admin1 = 10;                       // fipscode(subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
        public const int Admin2 = 11;                       // var admin2code = fields[i++].Trim();             // code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
        // i++;                                             // admin3 code       : code for third level administrative division, varchar(20)
        // i++;                                             // admin4 code       : code for fourth level administrative division, varchar(20)
        public const int Population = 14;                   // population        : bigint(8 byte int)
        // elevation         : in meters, integer
        // dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer.srtm processed by cgiar/ciat.
        // timezone          : the iana timezone id(see file timeZone.txt) varchar(40)
        // modification date : date of last modification in yyyy-MM-dd format


        //public const int alternateNameId   : the id of this alternate name, int
        public const int GeographicId = 1;      //         : geonameId referring to id in table 'geoname', int
        public const int Language = 2;          //      : iso 639 language code 2- or 3-characters, optionally followed by a hyphen and a countrycode for country specific variants (ex:zh-CN) or by a variant name (ex: zh-Hant); 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link to a website(mostly to wikipedia), wkdt for the wikidataid, varchar(7)
        public const int AlternatName = 3;      //    : alternate name or name variant, varchar(400)
        public const int IsPreferredName = 4;   //   : '1', if this alternate name is an official/preferred name
        public const int IsShortName = 5;       //       : '1', if this is a short name like 'California' for 'State of California'
        public const int IsColloquial = 6;      //      : '1', if this alternate name is a colloquial or slang term.Example: 'Big Apple' for 'New York'.
        public const int IsHistoric = 7;        //        : '1', if this alternate name is historic and was used in the past.Example 'Bombay' for 'Mumbai'.
        public const int From = 8;              // : from period when the name was used
        public const int To = 9;                //	  : to period when the name was used
    }

    private abstract class Entry
    {
        public readonly string[] Fields;
        public readonly long Id;
        public string Name => Fields[FieldIndex.Name];
        public string AsciiName => Fields[FieldIndex.AsciiName];
        public string CountryCode => Fields[FieldIndex.CountryCode];
        public long BenchmarkId { get; set; } = 0;
        public string Description => Fields[FieldIndex.AsciiName];
        public readonly long Population;

        protected Entry(string[] fields)
        {
            Fields = fields;
            Id = long.Parse(fields[FieldIndex.Id]);
            Population = long.Parse(Fields[FieldIndex.Population]);
        }

        public static string MakeKey(params object[] parts)
            => string.Join("$", parts);

        public override string ToString()
        {
            return $"{AsciiName} [{Id}]";
        }
    }

    private class Area : Entry
    {
        public readonly Dictionary<long, City> Cities = new ();

        public Area(string[] fields) : base(fields)
        {
        }
    }

    private class Country : Area
    {
        public readonly Dictionary<string, Division1> States = new(StringComparer.OrdinalIgnoreCase);

        public Country(string[] fields) : base(fields)
        { 
        }
    }

    private class Division1 : Area // 'State'
    {
        public string Admin1 => Fields[FieldIndex.Admin1];
        public readonly Dictionary<string, Division2> Counties = new(StringComparer.OrdinalIgnoreCase);

        public Division1(string[] fields) : base(fields)
        {
        }
    }

    private class Division2 : Area // 'County'
    {
        public string Admin1 => Fields[FieldIndex.Admin1];
        public string Admin2 => Fields[FieldIndex.Admin2];

        public Division2(string[] fields) : base(fields)
        {
        }
    }

    private class City : Entry 
    {
        public string Admin1 => Fields[FieldIndex.Admin1];
        public string Admin2 => Fields[FieldIndex.Admin2];

        public City(string[] fields) : base(fields)
        {
        }
    }

    private static readonly Dictionary<string, Country> Countries = new (StringComparer.OrdinalIgnoreCase);
    private static readonly Dictionary<long, Entry> AreasById = new ();
    private static string? _server = ".";
    private static string? _database = "AMBenchmark_DB";
    private static bool _quick = false;
    private static bool _dump = false;
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
            if (arg == "-quick")
                _quick = true;
            else if (arg == "-dump")
                _dump = true;
            else if (arg.StartsWith("-cutoff:"))
                ParseCmdLineValue(arg, ref _populationCutoff);
            else if (arg.StartsWith("-creator:"))
                ParseCmdLineValue(arg, ref _creatorId);
             else if (arg.StartsWith("-practice:"))
                ParseCmdLineValue(arg, ref _practiceAreaId);
           else if (_server == null)
                _server = arg;
            else if (_database == null)
                _database = arg;
            else
            {
                Console.WriteLine("ImportLocationsFromWeb [-quick] server database");
                return;
            }
        }

        _server ??= ".";
        _database ??= "AMBenchmark_DB";

        try
        {
            _connection = new AmbDbConnection($"Server={_server};Database={_database};Integrated Security=True;");
            ImportContinents();
            ImportGeographyLocations();
            ImportAliases();
            ImportCountryCodes();

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
        }
        catch (Exception e)
        {
            _connection?.Dispose();
            Log.Console = true;
            Log.Error(e);
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

    private static void ImportContinents()
    {
        const string c2c = "CountriesToContinents.txt";
        ProcessLines("../../../../../", c2c, (line) =>
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

    private static void ImportGeographyLocations()
    {
        const string allCountries = "allCountries";
        DownloadFile("https://download.geonames.org/export/dump/" + allCountries + ".zip");
        UnzipFile(allCountries + ".zip");

        using var cleaned = File.CreateText($"{allCountries}/processed.txt");
        var outputs = new Dictionary<string, StreamWriter>();

        // split the lines across several files
        ProcessLines(allCountries, allCountries + ".txt", (line) =>
        {
            var x = SplitData(line);
            if (x != "")
            {
                cleaned.WriteLine(line);
                if (!outputs.TryGetValue(x, out var output))
                    outputs.Add(x, output = File.CreateText($"{allCountries}/{x}.txt"));
                output.WriteLine(line);
            }
        });

        foreach (var output in outputs.Values)
            output.Close();

        ProcessLines(allCountries, "PCLI.txt", line => ProcessCountryRecord(line, false));
        ProcessLines(allCountries, "PCLD.txt", line => ProcessCountryRecord(line, true));
        ProcessLines(allCountries, "TERR.txt", line => ProcessCountryRecord(line, true));
        ProcessLines(allCountries, "PCLIX.txt", line => ProcessCountryRecord(line, false));
        ProcessLines(allCountries, "PCLS.txt", line => ProcessCountryRecord(line, false));
        ProcessLines(allCountries, "PCLF.txt", line => ProcessCountryRecord(line, false));
        ProcessLines(allCountries, "PCL.txt", line => ProcessCountryRecord(line, false));        
        ProcessLines(allCountries, "ADM1.txt", line => ProcessStateRecord(line, true));
        // ProcessLines(allCountries, "ADM1H.txt", line => ProcessStateRecord(line, true));
        ProcessLines(allCountries, "ADM2.txt", line => ProcessCountyRecord(line, true));
        // ProcessLines(allCountries, "ADM2H.txt", line => ProcessCountyRecord(line, true));
        ProcessLines(allCountries, "PPL.txt", line => ProcessCityRecord(line, true));
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
                Log.Error("ERROR: Unknown country: " + iso2 + " " + country + "    country (" + geonameid + ")");
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

        var featureClass = fields[6].Trim();            //    : see http://www.geonames.org/export/codes.html, char(1)
        if ((featureClass != "A") && (featureClass != "P") && featureClass != "")
            return "";
        var featureCode = fields[7].Trim();           // feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
        return (featureCode == "") ? "X" : featureCode.ToUpper();
    }

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
    private static string _inContenents = "";

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

        _inContenents  = "[PID] IN (" + string.Join(", ", _continents.Values) + ")";
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
