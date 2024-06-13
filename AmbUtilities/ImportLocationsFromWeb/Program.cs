using System.Diagnostics;
using System.Data;
using AmbHelper;


namespace ImportLocations
{
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
 
            protected Entry(string[] fields)
            {
                Fields = fields;
                Id = long.Parse(fields[FieldIndex.Id]);
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
            public readonly long Population;

            public City(string[] fields) : base(fields)
            {
                Population = long.Parse(Fields[FieldIndex.Population]);
            }
        }

        /*
        private class Alias
        {
            public readonly string[] Fields;
            public string Language => Fields[FieldIndex.Language]; 
            public string AlternatName => Fields[FieldIndex.AlternatName];
            //public const int IsPreferredName = 4;   //   : '1', if this alternate name is an official/preferred name
            //public const int IsShortName = 5;       //       : '1', if this is a short name like 'California' for 'State of California'
            // public const int IsColloquial = 6;      //      : '1', if this alternate name is a colloquial or slang term.Example: 'Big Apple' for 'New York'.
            public bool IsHistoric => Fields[FieldIndex.IsHistoric] == "1";
            //public readonly DateTime From;
            public readonly DateTime To;

            public Alias(string[] fields)
            {
                Fields = fields;
                if (Language != "")
                    Debugger.Break();
                Fields = fields;
                if (Fields[FieldIndex.To] == "")
                    To = DateTime.MaxValue;
                else
                    To = DateTime.Parse(Fields[FieldIndex.To]);
            }
        }
        */


        private static readonly HashSet<string> ExcludedFeatureCodes = new(StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<string, Country> Countries = new (StringComparer.OrdinalIgnoreCase);
        private static readonly Dictionary<long, Entry> AreasById = new ();
        //private static readonly Dictionary<string, Division1> States = new(StringComparer.OrdinalIgnoreCase);
        //private static readonly Dictionary<string, Division2> Counties = new(StringComparer.OrdinalIgnoreCase);

        /*
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
        */

        private static string? _server = ".";
        private static string? _database = "AMBenchmark_DB";
        private static bool _quick = false;
        private static AmbDbConnection? _connection;
        private const long PopulationCutoff = 1000;

        static void Main(string[] args)
        {
            Log.NoDebugger = true;
            Log.NoConsole = true;

            if (args.Length > 3)
            {
                Console.WriteLine("ImportLocationsFromWeb [q] server database");
                return;
            }

            foreach (var arg in args)
            {
                if (arg == "q")
                    _quick = true;
                else if (_server == null)
                    _server = arg;
                else if (_database == null)
                    _database = arg;
            }

            _server ??= ".";
            _database ??= "AMBenchmark_DB";

            try
            {
                //_connection = new AmbDbConnection($"Server{_server};Database={_database};Integrated Security=True;");
                ImportGeographyLocations();
                ImportAliases();
                //OrganizeData();
            }
            catch (Exception e)
            {
                Log.NoDebugger = false;
                Log.NoConsole = false;
                Log.WriteLine(e);
                throw;
            }
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
            ProcessLines(allCountries, "ADM1.txt", ProcessStateRecord);
            ProcessLines(allCountries, "ADM2.txt", ProcessCountyRecord);
            ProcessLines(allCountries, "PPL.txt", ProcessCityRecord);
        }

        private static void ProcessCountryRecord(string line, bool optional)
        {
            var fields = line.Split('\t');
            var countryCode = fields[FieldIndex.CountryCode].Trim();

            if (Countries.ContainsKey(countryCode))
            {
                if (optional)
                    return;
                Log.WriteLine("Duplicate country code: " + countryCode);
            }
            else
            {
                var country = new Country(fields);
                Countries.Add(country.CountryCode, country);
                AreasById.Add(country.Id, country);
                AddCountryToDatabase(country);
            }
        }

        private static void ProcessStateRecord(string line)
        {
            var fields = line.Split('\t');

            var state = new Division1(fields);
            if (Countries.TryGetValue(state.CountryCode, out var country))
            {
                if (country.States.ContainsKey(state.Admin1))
                {
                    Log.WriteLine("ERROR: Duplicate state code: " + state.CountryCode + "." + state.Admin1);
                }
                else
                {
                    country.States.Add(state.Admin1, state);
                    AreasById.Add(state.Id, state);
                    AddStateToDatabase(state);
                }
            }
            else
            {
                Log.WriteLine("ERROR: Unknown country code: " + state.CountryCode);
            }
        }

        private static void ProcessCountyRecord(string line)
        {
            var fields = line.Split('\t');

            var county = new Division2(fields);
            if (Countries.TryGetValue(county.CountryCode, out var country))
            {
                if (country.States.TryGetValue(county.Admin1, out var state))
                {
                    if (state.Counties.ContainsKey(county.Admin2))
                    {
                        Log.WriteLine("ERROR: Duplicate county code: " + county.CountryCode + "." + county.Admin1 + "." + county.Admin2);
                    }
                    else
                    {
                        state.Counties.Add(county.Admin2, county);
                        AreasById.Add(county.Id, county);
                        AddCountyToDatabase(county);
                    }
                }
                else
                {
                    Log.WriteLine("ERROR: Unknown state code: " + county.CountryCode + "." + county.Admin1);
                }
            }
            else
            {
                Log.WriteLine("ERROR: Unknown country code: " + county.CountryCode);
            }
        }

        private static void ProcessCityRecord(string line)
        {
            var fields = line.Split('\t');
            var city = new City(fields);
            if (city.Population >= PopulationCutoff)
            {
                if (Countries.TryGetValue(city.CountryCode, out var country))
                {
                    if (city.Admin1 == "")
                    {
                        country.Cities.Add(city.Id, city);
                        AreasById.Add(city.Id, city);
                        AddCityToDatabase(city, country);
                    }
                    else if (country.States.TryGetValue(city.Admin1, out var state))
                    {
                        if (city.Admin2 == "")
                        {
                            state.Cities.Add(city.Id, city);
                            AreasById.Add(city.Id, city);
                            AddCityToDatabase(city, state);
                        }
                        else if (state.Counties.TryGetValue(city.Admin2, out var county))
                        {
                            county.Cities.Add(city.Id, city);
                            AreasById.Add(city.Id, city);
                            AddCityToDatabase(city, county);
                        }
                        else
                        {
                            Log.WriteLine("ERROR: Unknown county code: " + city.CountryCode + "." + city.Admin1 + "." + city.Admin2);
                        }
                    }
                    else
                    {
                        Log.WriteLine("ERROR: Unknown state code: " + city.CountryCode + "." + city.Admin1);
                    }
                }
                else
                {
                    Log.WriteLine("ERROR: Unknown country code: " + city.CountryCode);
                }
            }
        }

        private static HashSet<string> _english = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private static HashSet<string> _englishLike = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private static HashSet<string> _foreign = new HashSet<string>(StringComparer.OrdinalIgnoreCase);


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

                if (fields[FieldIndex.IsHistoric] == "1")
                    return;
                var language = fields[FieldIndex.Language];
                if (language is "link" or "wkdt" or "post" or "iata" or "icao" or "faac" or "fr_1793")
                    return;

                var areaId = long.Parse(fields[FieldIndex.GeographicId]);
                if (!AreasById.TryGetValue(areaId, out var area))
                    return;

                var alias = fields[FieldIndex.AlternatName];
                bool isEnglish = true;
                //bool isEnglishLike = true;
                foreach (var ch in alias)
                {
                    if (ch > 0x007F)
                    {
                        isEnglish = false;
                        break;
                        //if (ch > 0x00FF)
                        //{
                            //isEnglishLike = false;
                            //break;
                        //}
                    }
                }

                if (isEnglish)
                {
                    //_english.Add(language);
                    AddEnglishAliasToDatabase(area!, alias);
                }
                //_foreign.Add(language);
                AddForeignAliasToDatabase(area!, alias, language);
            });

            /*
            foreach (var l in _foreign)
            {
                _english.Remove(l);
                _englishLike.Remove(l);
            }
            foreach (var l in _englishLike)
                _english.Remove(l);
            */
        }

        private static void AddCountryToDatabase(Country entry)
        {
        }

        private static void AddStateToDatabase(Division1 entry)
        {
        }

        private static void AddCountyToDatabase(Division2 entry)
        {
        }

        private static void AddCityToDatabase(City entry, Entry parent)
        {
        }

        private static void AddEnglishAliasToDatabase(Entry entry, string alias)
        {
        }


        private static void AddForeignAliasToDatabase(Entry entry, string alias, string language)
        {
        }


        private static string SplitData(string line)
        {
            var fields = line.Split('\t', StringSplitOptions.TrimEntries);
            if (fields.Length != 19)
            {
                Log.WriteLine($"Expected 19 fields, saw {fields.Length}");
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
                Log.WriteLine($"Exception while downloading file: {filename} from {url}");
                Log.WriteLine(ex);
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
                Log.WriteLine($"Exception while unzipping file: {zipFilename}");
                Log.WriteLine(ex);
                return false;
            }
        }

        private static void ProcessLines(string folder, string filename, Action<string> action)
        {
            try
            {
                var files = Directory.GetFiles(folder, filename);
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
                        // Log.WriteLine($"Processing line {line}").Indent();
                        ++line;
                        try
                        {
                            action(line2);
                        }
                        catch (Exception e)
                        {
                            Log.WriteLine($"Exception while processing line {line}");
                            Log.WriteLine(e);
                            Log.Outdent();
                            if (Debugger.IsAttached)
                                Debugger.Break();
                        }
                        finally
                        {
                            Log.Outdent();
                        }
                    }

                    Log.Outdent();
                    Log.WriteLine($"End of {filename} after {line} lines");
                    Log.WriteLine("Excluded feature codes: ").Indent();
                    var efc = ExcludedFeatureCodes.ToList();
                    efc.Sort();
                    foreach (var x in efc)
                        Log.WriteLine(x);
                    Log.Outdent();
                }
            }
            catch (Exception ex)
            {
                Log.Outdent();
                Log.WriteLine($"Exception while processing file: {filename}");
                Log.WriteLine(ex);
            }
        }
    }

#if false

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
            for (var row = top; row <= bottom; ++row)
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
            var children = _connection.Select<(long, string)>($"SELECT [OID], [Name] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parent.Oid}",
                reader => new(reader.GetInt64(0), reader.GetString(1)));

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
            var oldLog = _connection.Log;
            _connection.Log = false;
            const string filename = "dump.txt";

            if (File.Exists(filename))
                File.Delete(filename);

            using var file = File.CreateText(filename);
            Dump(0, "", file, new HashSet<long>());
            _connection.Log = oldLog;
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
            if (lastChildrenDumped != names[j - 1].oid)
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
#endif
}
