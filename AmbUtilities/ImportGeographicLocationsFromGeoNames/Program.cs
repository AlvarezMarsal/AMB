using AmbHelper;
using System.Globalization;
using static AmbHelper.Logs;

namespace ImportGeographicLocationsFromGeoNames;

internal partial class Program
{
    private string _server = ".";
    private string _benchmarkDatabase = "AMBenchmark_DB";
    private string _geoNamesDatabase = "GeoNames";
    //private int _line = 0;
    private AmbDbConnection? _connection;
    private AmbDbConnection Connection => _connection!;
    private static DateTime MinDateTime = new DateTime(1800, 1, 1);
    private static DateTime MaxDateTime = new DateTime(2999, 1, 1);
    private readonly DateTime _creationDate;
    private readonly string _creationDateAsString;
    private readonly Guid _creationSession = Guid.NewGuid();
    private readonly string _creationSessionAsString;
    private long _creatorId = 100;
    private int _practiceAreaId = 2501;
    private int _step = 0;
    // private bool _keep = false;
    private readonly HashSet<long> _geoNameIds = new ();
    private long _worldId = 20000;
    private readonly Dictionary<string, string> _continentNameToAbbreviation;
    private readonly Dictionary<string, string> _continentAbbreviationToName;
    private readonly Dictionary<string, long> _continentAbbreviationToId = new (StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> _geoNameCountryCodesToIds = new (StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<long, long> _geoNameCountryIdsToBenchmarkIds= new ();
    private readonly Dictionary<long, long> _previousChildIndex = new ();
    private readonly HashSet<long> _countryIds = new ();
    private string _workingFolder = "";

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
        _continentAbbreviationToName = new(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in _continentNameToAbbreviation)
            _continentAbbreviationToName[kvp.Value] = kvp.Key;
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
                _benchmarkDatabase = args[++i];
            else if (arg.StartsWith("-step"))
                _step = int.Parse(args[++i]);
            //else if (arg.StartsWith("-line"))
            //    _line = int.Parse(args[++i]);
            else if (arg.StartsWith("-work"))
                _workingFolder = args[++i];
            else if (_server == null)
                _server = arg;
            else if (_benchmarkDatabase == null)
                _benchmarkDatabase = arg;
            //else if (arg.StartsWith("-keep"))
            //    _keep = true;
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

        //if (_step > 0)
        //    _keep = true;

        if (_workingFolder == "")
            _workingFolder = Path.Combine(Path.GetTempPath(), "GeoNames");
        if (!Directory.Exists(_workingFolder))
            Directory.CreateDirectory(_workingFolder);

        _connection = new AmbDbConnection($"Server={_server};Database={_benchmarkDatabase};Integrated Security=True;");

        var done = false;
        for (/**/; !done; ++_step)
        {
            Log.WriteLine("STEP " + _step).Flush();
            Log.Indent();

            switch (_step)
            {
                /* GeoNames Import */
                case 10:
                    DownloadFiles();
                    break;

               case 20:
                    ImportFromAllCountriesFile();
                    break;

               case 40:
                    ImportFromCountryInfoFile();
                    break;

               case 50:
                    ImportCountriesToContinentsFile();
                    break;

               case 60:
                    ImportAlternateNamesFile();
                    done = true;
                    break;

                case 70:
                    WriteImportSql();
                    done = true;
                    break;

#if false
                /* Benchmark Import */
                case 70: 
                    ImportContinents(); 
                    break;

                case 80: 
                    ImportCountries(); 
                    break;

                case 90: 
                    ImportStates(); 
                    break;

                case 100: 
                    ImportCounties(); 
                    break;

                case 110: 
                    ImportCities(); 
                    break;

                case 120: 
                    ImportAliases(); 
                    break;

                case 199: 
                    Dump(); 
                    done = true;
                    break;
#endif
            }

            Log.Outdent();
            Log.Flush();
        }
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
}