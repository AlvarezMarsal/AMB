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
}