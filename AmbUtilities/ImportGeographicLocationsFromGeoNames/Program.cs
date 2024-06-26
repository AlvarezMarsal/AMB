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
    private static readonly DateTime CreationDate = DateTime.Now;
    /*
    private static readonly string CreationDateAsString;
    */
    private long _creatorId = 100;
    private int _practiceAreaId = 2501;
    private int _step = 0;
    private bool _keep = false;
    /*
    private static readonly Guid CreationSession = Guid.NewGuid();
    private static readonly string CreationSessionAsString;
    // private static long _populationCutoff = 1000;
    private static Dictionary<string, string> CountryToContinent = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    private static readonly List<string> delayedStates = [];
    private static readonly List<string> delayedCounties = [];
    private static readonly List<string> delayedCities = [];
    private static HashSet<long> UninhabitedCounties = new HashSet<long>();
    private static HashSet<long> UninhabitedStates = new HashSet<long>();
    */
    private static HashSet<long>? GeoNameIds;
    /*
    static Program()
    {
        CreationDateAsString = CreationDate.ToString();
        CreationSessionAsString = CreationSession.ToString();
    }
    */

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

        if (!_keep)
            CreateDatabaseTables();

        var done = false;
        for (/**/; !done; ++_step)
        {
            Log.WriteLine("STEP " + _step);
            Log.Indent();

            switch (_step)
            {
                case 0:
                    DownloadFiles();
                    break;

                case 1:
                    ImportFromGeoNamesFile("allcountries\\allcountries.txt");
                    break;

                case 2:
                    ImportFromGeoNamesFile("cities500\\cities500.txt", f =>
                    {
                        // If the population is less than 500, set it to 500
                        var pop = long.TryParse(f[14], out var p) ? p : 0;
                        if (pop < 500)
                            f[14] = "500";
                    });
                    break;

               case 3:
                    ImportGeoNamesCountryInfo("countryInfo.txt");
                    break;

               case 4:
                    ImportCountriesToContinents("..\\CountriesToContinents.txt");
                    break;

               case 5:
                    SetUpGeoNameIds();
                    ImportFlatTextFile("alternateNamesV2\\alternateNamesV2.txt", '\t', 10, (lineNumber, fields) =>
                    {
                        var i = 0;
                        var alternateNameId = long.Parse(fields[i++]);
                        var geonameId = long.Parse(fields[i++]);
                        if ((GeoNameIds == null) || GeoNameIds.Contains(geonameId))
                        {
                            var language = fields[i++];
                            if (language is "he" or "ru" or "ar" or "uk" or "zh" or "tt" 
                                                 or "kk" or "fr_1793" or "fa" or "el" 
                                                 or "xmf" or "bg" or "link" or "wkdt")
                                return;
                            var alternateName = fields[i++];
                            if (TryTranslate(alternateName, out var translated))
                            {
                                var isPreferredName = fields[i++] == "1";
                                var isShortName = fields[i++] == "1";
                                var isColloquial = fields[i++] == "1";
                                var isHistoric = fields[i++] == "1";

                                var from = ParseDateTime(fields[i++], MinDateTime);
                                var to = ParseDateTime(fields[i++], MaxDateTime);

                                if ((CreationDate >= from) && (CreationDate < to))
                                    InsertGeoNamesAlternateName(0, geonameId, language, translated!, isPreferredName, isShortName, isColloquial, isHistoric, from, to, lineNumber);
                            }
                        }
                    });
                    break;

                /*
                case 0: ImportContinents(); break;
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
        }

        /*
//            Log.WriteLine("Uninhabited counties: " + UninhabitedCounties.Count);
//            Log.WriteLine("Uninhabited states: " + UninhabitedStates.Count);

            if (_dump)
                Dump();
        */
    }

    private void SetUpGeoNameIds()
    {
        if (GeoNameIds != null)
            return;
        GeoNameIds = new HashSet<long>();
        Connection.ExecuteReader("SELECT GeoNameId FROM [dbo].[t_GeoNames]", r => GeoNameIds!.Add(r.GetInt64(0)));
    }

    private static DateTime ParseDateTime(string text, DateTime defaultValue)
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
            try
            {
                return new DateTime(year, (dm / 100), dm % 100);
            }
            catch
            {
                return defaultValue;
            }
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

    #region Download Files

    private void DownloadFiles()
    {
        DownloadAndUnzip("allCountries.zip");
        DownloadAndUnzip("cities500.zip");
        DownloadAndUnzip("countryInfo.txt");
        DownloadAndUnzip("alternateNamesV2.zip");

        void DownloadAndUnzip(string filename)
        {
            DownloadFile("https://download.geonames.org/export/dump/" + filename);
            if (filename.EndsWith(".zip"))
                UnzipFile(filename);
        }
    }

    private bool DownloadFile(string url)
    {
        var slash = url.LastIndexOf('/');
        var filename = url.Substring(slash + 1);

        try
        {
            Log.WriteLine($"Downloading file: {filename} from {url}");
            using var client = new HttpClient();
            using var s = client.GetStreamAsync(url);
            using var fs = new FileStream(filename, FileMode.OpenOrCreate);
            s.Result.CopyTo(fs);
            Log.WriteLine($"Download complete");
            return true;
        }
        catch (Exception ex)
        {
            Error.WriteLine($"Exception while downloading file: {filename} from {url}", ex);
            return false;
        }
    }

    private bool UnzipFile(string zipFilename, string? directory = null)
    {
        var name = Path.GetFileNameWithoutExtension(zipFilename);
        try
        {
            directory ??= name;

            if (Directory.Exists(directory))
                Directory.Delete(directory, true);
            Directory.CreateDirectory(directory);

            System.IO.Compression.ZipFile.ExtractToDirectory(zipFilename, directory + "\\");
            Log.WriteLine($"Unzipped file: {zipFilename}");
            return true;
        }
        catch (Exception ex)
        {
            Error.WriteLine($"Exception while unzipping file: {zipFilename}", ex);
            return false;
        }
    }

    #endregion

    #region Database creation

    private void CreateDatabaseTables()
    {
        Connection.ExecuteNonQuery(
           """
                IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_GeoNames]') AND type in (N'U'))
                    DROP TABLE [dbo].[t_GeoNames]
            """);

        Connection.ExecuteNonQuery(
           """
                CREATE TABLE [dbo].[t_GeoNames](
                    [GeoNameId] bigint NOT NULL,
                    [Name] nvarchar(200) NOT NULL,
                    [AsciiName] nvarchar(200) NOT NULL,
                    [AlternateNames] nvarchar(MAX) NOT NULL,
                    [Latitude] [float] NOT NULL,
                    [Longitude] [float] NOT NULL,
                    [FeatureClass] [nchar] NOT NULL,
                    [FeatureCode] nvarchar(10) NOT NULL,
                    [CountryCode] nchar(2) NOT NULL,
                    [CC2] nvarchar(200) NOT NULL,
                    [Admin1Code] nvarchar(20) NOT NULL,
                    [Admin2Code] nvarchar(80) NOT NULL,
                    [Admin3Code] nvarchar(20) NOT NULL,
                    [Admin4Code] nvarchar(20) NOT NULL,
                    [Admin5Code] nvarchar(20) NOT NULL,
                    [Population] bigint NOT NULL,
                    [Elevation] int NOT NULL,
                    [Dem] nvarchar(20) NOT NULL,
                    [Timezone] nvarchar(40) NOT NULL,
                    [ModificationDate] datetime NOT NULL,
                    [Continent] nchar(2) NOT NULL,
                    [BenchmarkId] bigint NOT NULL,
                    [LineNumber] int NOT NULL

                    CONSTRAINT [PK_t_GeoNames] PRIMARY KEY CLUSTERED 
                    (
                        [GeoNameId] ASC
                    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                ) ON [PRIMARY]
            """);

         Connection.ExecuteNonQuery(
            """
                IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[t_GeoNamesAlternateNames]') AND type in (N'U'))
                    DROP TABLE [dbo].[t_GeoNamesAlternateNames]
            """);

        Connection.ExecuteNonQuery(
           """
                CREATE TABLE [dbo].[t_GeoNamesAlternateNames](
                    [AlternateNameId] bigint NULL,
                    [GeoNameId] bigint NOT NULL,
                    [Language] nvarchar(7) NOT NULL,
                    [AlternateName] nvarchar(400) NOT NULL,
                    [IsPreferredName] bit NOT NULL,
                    [IsShortName] bit NOT NULL,
                    [IsColloquial] bit NOT NULL,
                    [IsHistoric] bit NOT NULL,
                    [From] datetime NOT NULL,
                    [To] datetime NOT NULL,
                    [LineNumber] int NOT NULL

                    CONSTRAINT [PK_t_GeoNamesAlternateNames] PRIMARY KEY CLUSTERED 
                    (
                        [GeoNameId] ASC,
                        [AlternateName] ASC
                    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
                ) ON [PRIMARY]
            """);

        Connection.ExecuteNonQuery(
           """
                CREATE OR ALTER PROCEDURE [dbo].[sp_InsertUpdateGeoName]
                    @GeoNameId bigint,
                    @Name nvarchar(200),
                    @AsciiName  nvarchar(200),
                    @AlternateNames  nvarchar(MAX),
                    @Latitude  float ,
                    @Longitude  float ,
                    @FeatureClass  nchar ,
                    @FeatureCode  nvarchar(10),
                    @CountryCode  nchar(2),
                    @CC2  nvarchar(200),
                    @Admin1Code  nvarchar(20),
                    @Admin2Code  nvarchar(80),
                    @Admin3Code  nvarchar(20),
                    @Admin4Code  nvarchar(20),
                    @Admin5Code  nvarchar(20),
                    @Population  bigint,
                    @Elevation  int,
                    @Dem  nvarchar(20),
                    @Timezone  nvarchar(40),
                    @ModificationDate  datetime,
                    @Continent  nchar(2),
                    @LineNumber int
                AS
                BEGIN
                    IF NOT EXISTS (SELECT * FROM [dbo].[t_GeoNames] WHERE [GeoNameId] = @GeoNameId)
                    BEGIN
                        INSERT INTO [dbo].[t_GeoNames] (
                            [GeoNameId], [Name], [AsciiName], [AlternateNames], [Latitude], [Longitude], [FeatureClass], [FeatureCode], 
                            [CountryCode], [CC2], [Admin1Code], [Admin2Code], [Admin3Code], [Admin4Code], [Admin5Code], [Population], [Elevation], 
                            [Dem], [Timezone], [ModificationDate], [Continent], [BenchmarkId], [LineNumber]
                        ) VALUES (
                            @GeoNameId , @Name , @AsciiName , @AlternateNames , @Latitude , @Longitude , @FeatureClass , @FeatureCode , 
                            @CountryCode , @CC2 , @Admin1Code , @Admin2Code , @Admin3Code , @Admin4Code , @Admin5Code , @Population , @Elevation , 
                            @Dem, @Timezone, @ModificationDate, @Continent, 0, @LineNumber
                        )
                    END
                    ELSE
                    BEGIN
                        UPDATE [dbo].[t_GeoNames]
                            SET [Population] = @Population
                        WHERE [GeoNameId] = @GeoNameId AND [Population] < @Population
                    END
                END            
            """);

        Connection.ExecuteNonQuery(
           """
                CREATE OR ALTER PROCEDURE [dbo].[sp_InsertUpdateGeoNameAlternateName]
                    @AlternateNameId bigint = 0,
                    @GeoNameId bigint,
                    @Language nvarchar(7) = N'',
                    @AlternateName nvarchar(400),
                    @IsPreferredName bit = 0,
                    @IsShortName bit = 0,
                    @IsColloquial bit = 0,
                    @IsHistoric bit = 0,
                    @From datetime = '1700-01-01',
                    @To datetime = '2999-12-31',
                    @LineNumber int
                AS
                BEGIN
                    IF NOT EXISTS (SELECT * FROM [dbo].[t_GeoNamesAlternateNames] WHERE [GeoNameId] = @GeoNameId AND [AlternateName] = @AlternateName)
                    BEGIN
                        IF EXISTS (SELECT * FROM [dbo].[t_GeoNames] WHERE [GeoNameId] = @GeoNameId)
                        BEGIN
                            INSERT INTO [dbo].[t_GeoNamesAlternateNames] (
                                [AlternateNameId], [GeoNameId], [Language], [AlternateName], [IsPreferredName], [IsShortName], [IsColloquial], [IsHistoric], [From], [To], [LineNumber]
                            ) VALUES (
                                @AlternateNameId, @GeoNameId, @Language, @AlternateName, @IsPreferredName, @IsShortName, @IsColloquial,@IsHistoric, @From, @To, @LineNumber
                            )
                        END
                    END
                END
            """);
    }


    private void InsertGeoNamesAlternateName(long altenativeNameId, long geonameId, string language, string name,
                    bool isPreferredName, bool isShortName, bool isColloguial, bool isHistoric,
                    DateTime from, DateTime to, int lineNumber)
    {
        using var command = Connection.CreateCommand("[dbo].[sp_InsertUpdateGeoNameAlternateName]", false);
        command.CommandType = CommandType.StoredProcedure;

        command.Parameters.Add("@AlternateNameId", SqlDbType.BigInt, 8).Value = altenativeNameId;
        command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8).Value = geonameId;
        command.Parameters.Add("@Language", SqlDbType.NVarChar, 7).Value = language;
        command.Parameters.Add("@AlternateName", SqlDbType.NText).Value = name;
        command.Parameters.Add("@IsPreferredName", SqlDbType.Bit).Value = isPreferredName ? 1 : 0;
        command.Parameters.Add("@IsShortName", SqlDbType.Bit).Value = isShortName ? 1 : 0;
        command.Parameters.Add("@IsColloquial", SqlDbType.Bit).Value = isColloguial ? 1 : 0;
        command.Parameters.Add("@IsHistoric", SqlDbType.Bit).Value = isHistoric ? 1 : 0;
        command.Parameters.Add("@From", SqlDbType.DateTime).Value = from;
        command.Parameters.Add("@To", SqlDbType.DateTime).Value = to;
        command.Parameters.Add("@LineNumber", SqlDbType.Int).Value = lineNumber;

        Connection.ExecuteNonQuery(command, false);
    }

    private void InsertGeoNamesAlternateName(long geonameId, string name, int lineNumber)
        => InsertGeoNamesAlternateName(0, geonameId, "", name, false, false, false, false, MinDateTime, MaxDateTime, lineNumber);

    #endregion

    /*
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
    */

    #region Import From GeoNames

    private void ImportFromGeoNamesFile(string filename)
        => ImportFromGeoNamesFile(filename, _ => { });
 
    private void ImportFromGeoNamesFile(string filename, Action<string[]>? updateFields)
    {
        ImportFlatTextFile(filename, '\t', 19, (lineNumber, fields) =>
        {
            if (fields[6].Length < 1)
                return;
            var featureClass = fields[6][0];
            if (featureClass is not ('A' or 'P'))
                return;

            if (updateFields != null)
                updateFields(fields);

            using var command = Connection.CreateCommand("[dbo].[sp_InsertUpdateGeoName]");
            command.CommandType = CommandType.StoredProcedure;
            var i = 0;
            command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8).Value = long.Parse(fields[i++]);
            command.Parameters.Add("@Name", SqlDbType.NVarChar, 200).Value = fields[i++];
            command.Parameters.Add("@AsciiName", SqlDbType.NVarChar, 200).Value = fields[i++];
            command.Parameters.Add("@AlternateNames", SqlDbType.NText).Value = fields[i++];
            command.Parameters.Add("@Latitude", SqlDbType.Float).Value = double.Parse(fields[i++]);
            command.Parameters.Add("@Longitude", SqlDbType.Float).Value = double.Parse(fields[i++]);
            command.Parameters.Add("@FeatureClass", SqlDbType.NChar).Value = featureClass; 
            i++;
            command.Parameters.Add("@FeatureCode", SqlDbType.NVarChar, 10).Value = fields[i++];
            command.Parameters.Add("@CountryCode", SqlDbType.NVarChar, 2).Value = fields[i++];
            command.Parameters.Add("@CC2", SqlDbType.NVarChar, 200).Value = fields[i++];
            command.Parameters.Add("@Admin1Code", SqlDbType.NVarChar, 20).Value = fields[i++];
            command.Parameters.Add("@Admin2Code", SqlDbType.NVarChar, 80).Value = fields[i++];
            command.Parameters.Add("@Admin3Code", SqlDbType.NVarChar, 20).Value = fields[i++];
            command.Parameters.Add("@Admin4Code", SqlDbType.NVarChar, 20).Value = fields[i++];
            command.Parameters.Add("@Admin5Code", SqlDbType.NVarChar, 20).Value = "";
            command.Parameters.Add("@Population", SqlDbType.BigInt, 8).Value = long.Parse(fields[i++]);
            command.Parameters.Add("@Elevation", SqlDbType.Int, 4).Value = int.TryParse(fields[i++], out var elevation) ? elevation : 0;
            command.Parameters.Add("@Dem", SqlDbType.NVarChar, 20).Value = fields[i++];
            command.Parameters.Add("@Timezone", SqlDbType.NVarChar, 40).Value = fields[i++];
            command.Parameters.Add("@ModificationDate", SqlDbType.DateTime).Value = DateTime.Parse(fields[i++]);
            command.Parameters.Add("@Continent", SqlDbType.NVarChar, 2).Value = "";
            command.Parameters.Add("@LineNumber", SqlDbType.Int).Value = lineNumber;
            command.ExecuteNonQuery();
        });

    }

    private void ImportGeoNamesCountryInfo(string filename)
    {
        ImportFlatTextFile(filename, '\t', 19, (lineNumber, fields) =>
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

            InsertGeoNamesAlternateName(geonameId, iso, lineNumber);
            InsertGeoNamesAlternateName(geonameId, iso3, lineNumber);
            InsertGeoNamesAlternateName(geonameId, country, lineNumber);

            var result = Connection.ExecuteNonQuery(
                $"""
                    UPDATE [dbo].[t_GeoNames]
                        SET [Continent] = N'{continent}'
                        WHERE [GeoNameId] = {geonameId} OR CountryCode = N'{iso}'
                 """);
        });
    }
    
    private void ImportFlatTextFile(string filename, char separator, int fieldCount, Action<int, string[]> process)
    {
        var f = File.OpenText(filename);
        var lineNumber = 0;
        while (true)
        {
            var line = f.ReadLine();
            if (line == null)
                break;
            if (++lineNumber < _line)
                continue;
            if ((lineNumber % 100000) == 0)
                Log.WriteLine($"Line {lineNumber}");
            _line = 0;
            if (line.StartsWith("#"))
                continue;
            try
            {
                var fields = line.Split(separator, StringSplitOptions.TrimEntries);
                if (fields.Length != fieldCount)
                {
                    Error.WriteLine($"Expected {fieldCount} fields, saw {fields.Length} for line {lineNumber}");
                    continue;
                }

                process(lineNumber, fields);
            }
            catch (Exception e)
            {
                Error.WriteLine($"Exception while processing line {lineNumber}", e);
            }
        }

    }

    #endregion

    #region Import local data

   private void ImportCountriesToContinents(string filename)
    {
        ImportFlatTextFile(filename, '\t', 3, (_, fields) =>
        {
            var iso = fields[0];
            var country = fields[1];
            var continent = fields[2];

            var bmContinent = continent switch 
            {
                "Europe" => "EU",
                "Middle East" => "ME",
                "Central Asia" => "CA",
                "North America" => "NA",
                "Latin America" => "LA",
                "Africa" => "AF",
                "Antarctica" => "AN",
                "Southeast Asia & Australia (SEAA)" => "SA",
                "North Asia" => "NO",
                _ => throw new Exception("Unknown continent: " + continent)
            };


            var result = Connection.ExecuteNonQuery(
                $"""
                    UPDATE [dbo].[t_GeoNames]
                        SET [Continent] = N'{bmContinent}'
                        WHERE CountryCode = N'{iso}'
                 """);
        });
    }

    #endregion

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

    private static bool TryTranslate(string name, out string? translation)
    {
        var n = name.Normalize(System.Text.NormalizationForm.FormD);
        var a = n.ToCharArray()
            .Where(c => (CharUnicodeInfo.GetUnicodeCategory(c) != UnicodeCategory.NonSpacingMark) && (c < 0x007F))
            .ToArray();
        if (a.Length != name.Length)
        {
            translation = name;
            return false;
        }
        //translation = new string(a);
        translation = name;
        return true;
    }

    /*
    private static string SplitData(string line)
    {
        var fields = line.Split('\t', StringSplitOptions.TrimEntries);
        if (fields.Length != 19)
        {
            Error.WriteLine($"Expected 19 fields, saw {fields.Length}");
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
            Error.WriteLine($"Exception while downloading file: {filename} from {url}");
            Error.WriteLine(ex);
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
            Error.WriteLine($"Exception while unzipping file: {zipFilename}");
            Error.WriteLine(ex);
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
                        Error.WriteLine($"Exception while processing line {line}");
                        Error.WriteLine(e);
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
            Error.WriteLine($"Exception while processing file: {filename}");
            Error.WriteLine(ex);
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

*/
}