using AmbHelper;
using ImportJobTitles;

namespace ImportLocations
{
    internal class Program
    {
        private readonly Settings _settings;
        private readonly DateTime _startTime = DateTime.Now;
        private readonly AmbDbConnection _connection;
        //private readonly Guid _creationSession;
        //private readonly SortedList<string, HashSet<long>> _aliases = [];
        //private string NameCollation => "COLLATE " + _settings.NameCollation;
        //private string AliasCollation => "COLLATE " + _settings.AliasCollation;
        
        static Program()
        {
        }

        static void Main(string[] args)
        {
            var exe = Environment.GetCommandLineArgs()[0];
            var filename = Path.GetFileNameWithoutExtension(exe);

            if (args.Length > 1)
            {
                Console.WriteLine($"{filename} [filename]");
                Console.WriteLine($"    filename is the name of a JSON settings file.");
                Console.WriteLine($"    If omitted, it defaults to {filename}.json.");
               return;
            }

            var arg0 = (args.Length > 0) ? args[0] : $"{filename}.json";
            var settingsFileName = Path.GetFullPath(arg0);
            if (!File.Exists(settingsFileName))
            {
                Log.WriteLine($"Settings file not found: {settingsFileName}");
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
                Log.WriteLine(e.ToString());
                throw;
            }
        }

        private Program(Settings settings)
        {
            _settings = settings;
            _connection = new AmbDbConnection(_settings.ConnectionString);
        }

        private void Run()
        {
            using (_connection)
            {
                try
                {
                    CreateViews();
                    //EnforcePresets();

                    foreach (var import in _settings.Imports)
                    {
                        ProcessImport(import, _connection);
                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
                finally
                {
                    //Dump();
                    DeleteViews();
                }
            }
        }

        private void CreateViews()
        {
            var view = $"""
                        CREATE OR ALTER VIEW [dbo].[vw_TaxonomyNode]
                        AS
                            SELECT P.[OID], P.[PID], P.[Index], P.[Name], 't_TaxonomyNodeProcess' AS [Table] FROM [dbo].[t_TaxonomyNodeProcess] P
                            UNION
                            SELECT Q.[OID], Q.[PID], Q.[Index], Q.[Name], 't_TaxonomyNodeProcessGroup' AS [Table] FROM [dbo].[t_TaxonomyNodeProcessGroup] Q
                            UNION
                            SELECT F.[OID], F.[PID], F.[Index], F.[Name], 't_TaxonomyNodeFunction' AS [Table] FROM [dbo].[t_TaxonomyNodeFunction] F
                            UNION
                            SELECT G.[OID], G.[PID], G.[Index], G.[Name], 't_TaxonomyNodeFunctionGroup' AS [Table] FROM [dbo].[t_TaxonomyNodeFunctionGroup] G
                        """;
            _connection.ExecuteNonQuery(view);
        }

        private void DeleteViews()
        {
            var drop = "DROP VIEW [dbo].[vw_TaxonomyNode]";
            _connection.ExecuteNonQuery(drop);
        }

        #if false
        private void EnforcePresets()
        {
            foreach (var preset in _settings.Presets)
            {
                try
                {
                    var pid = preset.Pid.GetValueOrDefault(0);
                    if (pid != 0)
                        LoadGeographicLocation(pid, true, 1);

                    var n = preset.Name.Replace("'", "''");
                    var query = $"SELECT [OID], [PID] FROM [dbo].[vw_GeographicLocationNames] " +
                                $"WHERE [NAME] = N'{n}' {NameCollation} AND [PID] = {pid}";
                    if (preset.Oid != 0)
                        query += $" AND [OID] = {preset.Oid}";

                    using (var command = new SqlCommand(query, _connection))
                    {
                        using var reader = command.ExecuteReader();
                        if (reader.Read())
                        {
                            var oid = reader.GetInt64(0);
                            if ((preset.Oid != 0) && (preset.Oid != oid))
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            var npid = reader.GetInt64(1);
                            if (npid != pid)
                                throw new InvalidOperationException($"Missing preset {preset.Name}");
                            continue;
                        }
                    }

                    AddGeographicLocation(0, pid, 2501, preset.Name, true);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    if (Debugger.IsAttached)
                        Debugger.Break();
                    throw;
                }
            }
        }
#endif

        private void ProcessImport(Settings.Import import, AmbDbConnection connection)
        {
            if (!File.Exists(import.FilePath))
                throw new FileNotFoundException(import.FilePath);

            Log.WriteLine($"Processing {import.FilePath}");
            using var importer = new ImportProcessor(_settings, import, connection);
            importer.Run();
        }
    }
}
