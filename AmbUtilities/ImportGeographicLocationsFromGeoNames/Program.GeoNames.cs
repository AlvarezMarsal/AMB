using System.Data;
using System.Globalization;
using static AmbHelper.Logs;
using System.Diagnostics;

namespace ImportGeographicLocationsFromGeoNames;

internal partial class Program
{
    private readonly HashSet<string> _targetedFeatureCodesAdmin = new(StringComparer.OrdinalIgnoreCase)
    {
        "ADM1",     // first-order administrative division
        // "ADM1H", // historical first-order administrative division	a former first-order administrative division
        "ADM2",     // second-order administrative division
        // "ADM2H", // historical second-order administrative division	a former second-order administrative division
        // "ADM3",  // third-order administrative division	a subdivision of a second-order administrative division
        // "ADM3H", // historical third-order administrative division	a former third-order administrative division
        // "ADM4",  // fourth-order administrative division	a subdivision of a third-order administrative division
        // "ADM4H", // historical fourth-order administrative division	a former fourth-order administrative division
        // "ADM5",  // fifth-order administrative division	a subdivision of a fourth-order administrative division
        // "ADM5H", // historical fifth-order administrative division	a former fifth-order administrative division
        "ADMD",     // administrative division
        // "ADMDH", // historical administrative division 	a former administrative division of a political entity, undifferentiated as to administrative level
        // "LTER",  // leased area	a tract of land leased to another country, usually for military installations


    };

    private readonly HashSet<string> _targetedFeatureCodesCountry = new(StringComparer.OrdinalIgnoreCase)
    {
        // "PCL",      // political entity	
        "PCLD",     // dependent political entity
        "PCLF",     // freely associated state
        // "PCLH",     // historical political entity	a former political entity
        "PCLI",     // independent political entity
        "PCLIX",    // section of independent political entity
        "PCLS",     // semi-independent political entity
        // "PRSH",  // parish	an ecclesiastical district
        "TERR"      // territory
    };

    private readonly HashSet<string> _targetedFeatureCodesCity = new(StringComparer.OrdinalIgnoreCase)
    {
        "PPL",      // populated place
        "PPLA",     // seat of a first-order administrative division
        "PPLA2",    // seat of a second-order administrative division            
        "PPLA3",    // seat of a third-order administrative division
        "PPLA4",    // seat of a fourth-order administrative division	
        "PPLA5",    // seat of a fifth-order administrative division	
        "PPLC",     // capital of a political entity	
        // "PPLCH",    // historical capital of a political entity	a former capital of a political entity
        "PPLF",     // farm village	a populated place where the population is largely engaged in agricultural activities
        "PPLG",     // seat of government of a political entity	
        // "PPLH",     // historical populated place	a populated place that no longer exists
        "PPLL",     // populated locality	an area similar to a locality but with a small group of dwellings or other buildings
        // "PPLQ",     // abandoned populated place	
        "PPLR",     // religious populated place	a populated place whose population is largely engaged in religious occupations
        "PPLS",     // populated places	cities, towns, villages, or other agglomerations of buildings where people live and work
        // "PPLW",     // destroyed populated place	a village, town or city destroyed by a natural disaster, or by war
        "PPLX",     // section of populated place	
        "STLMT",    // israeli settlement	
    };

    #region Download Files

    private void DownloadFiles()
    {
        DownloadFile("https://download.geonames.org/export/dump/" + "cities500.zip");
        UnzipFile("cities500.zip");
        DownloadFile("https://download.geonames.org/export/dump/" + "countryInfo.txt");
        DownloadFile("https://download.geonames.org/export/dump/" + "admin1CodesASCII.txt");
        DownloadFile("https://download.geonames.org/export/dump/" + "admin2Codes.txt");
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
            var targetFilename = Path.Combine(_workingFolder, filename);
            using var fs = new FileStream(targetFilename, FileMode.OpenOrCreate);
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

    private bool UnzipFile(string zipFilename)
    {
        var name = Path.GetFileNameWithoutExtension(zipFilename);
        try
        {
            System.IO.Compression.ZipFile.ExtractToDirectory(zipFilename, _workingFolder);
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

    #region Import From GeoNames

    #if false
    private void ImportFromAllCountriesFile()
    {
        LoadGeoNameIds();
        var allFeatureCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fc in _targetedFeatureCodesAdmin)
            allFeatureCodes.Add(fc);
        foreach (var fc in _targetedFeatureCodesCountry)
            allFeatureCodes.Add(fc);
        foreach (var fc in _targetedFeatureCodesCity)
            allFeatureCodes.Add(fc);

        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertEntity]");
        command.CommandType = CommandType.StoredProcedure;
        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 200);
        var asciiNameParam = command.Parameters.Add("@AsciiName", SqlDbType.NVarChar, 200);
        var alternateNamesParam = command.Parameters.Add("@AlternateNames", SqlDbType.NText);
        var latitudeParam = command.Parameters.Add("@Latitude", SqlDbType.Float);
        var longitudeParam = command.Parameters.Add("@Longitude", SqlDbType.Float);
        var featureClassParam = command.Parameters.Add("@FeatureClass", SqlDbType.NChar);
        var featureCodeParam = command.Parameters.Add("@FeatureCode", SqlDbType.NVarChar, 10);
        var countryCodeParam = command.Parameters.Add("@CountryCode", SqlDbType.NVarChar, 2);
        var cc2Param = command.Parameters.Add("@CC2", SqlDbType.NVarChar, 200);
        var admin1CodeParam = command.Parameters.Add("@Admin1Code", SqlDbType.NVarChar, 20);
        var admin2CodeParam = command.Parameters.Add("@Admin2Code", SqlDbType.NVarChar, 80);
        var admin3CodeParam = command.Parameters.Add("@Admin3Code", SqlDbType.NVarChar, 20);
        var admin4CodeParam = command.Parameters.Add("@Admin4Code", SqlDbType.NVarChar, 20);
        var admin5CodeParam = command.Parameters.Add("@Admin5Code", SqlDbType.NVarChar, 20);
        var populationParam = command.Parameters.Add("@Population", SqlDbType.BigInt, 8);
        var elevationParam = command.Parameters.Add("@Elevation", SqlDbType.Int, 4);
        var demParam = command.Parameters.Add("@Dem", SqlDbType.NVarChar, 20);
        var timezoneParam = command.Parameters.Add("@Timezone", SqlDbType.NVarChar, 40);
        var modificationDateParam = command.Parameters.Add("@ModificationDate", SqlDbType.DateTime);
        var continentParam = command.Parameters.Add("@Continent", SqlDbType.NVarChar, 2);
        var lineNumberParam = command.Parameters.Add("@LineNumber", SqlDbType.Int);

        ImportFlatTextFile("allcountries\\allcountries.txt", '\t', 19, (lineNumber, fields) =>
        {
            var geoNameId = long.Parse(fields[0]);
            if (_geoNameIds.Contains(geoNameId))
                return false;
            if (!allFeatureCodes.Contains(fields[7]))
                return false;
            
            geoNameIdParam.Value = geoNameId;
            var i = 1;
            nameParam.Value = fields[i++];
            asciiNameParam.Value = fields[i++];
            alternateNamesParam.Value = fields[i++];
            latitudeParam.Value = double.Parse(fields[i++]);
            longitudeParam.Value = double.Parse(fields[i++]);
            var featureClass = fields[i++];
            featureClassParam.Value = (featureClass.Length == 0) ? ' ' : featureClass[0];
            featureCodeParam.Value = fields[i++];
            countryCodeParam.Value = fields[i++];
            cc2Param.Value = fields[i++];
            admin1CodeParam.Value = fields[i++];
            admin2CodeParam.Value = fields[i++];
            admin3CodeParam.Value = fields[i++];
            admin4CodeParam.Value = fields[i++];
            admin5CodeParam.Value = "";
            populationParam.Value = long.Parse(fields[i++]);
            elevationParam.Value = int.TryParse(fields[i++], out var elevation) ? elevation : 0;
            demParam.Value = fields[i++];
            timezoneParam.Value = fields[i++];
            modificationDateParam.Value = DateTime.TryParse(fields[i++], out var md) ? md : _creationDate;
            continentParam.Value = "";
            lineNumberParam.Value = lineNumber;
            var result = Connection.ExecuteNonQuery(command, false);
            Debug.Assert(result > 0);
            _geoNameIds.Add(geoNameId);
            return true;
        });
    }
    #endif

    private void ImportFromAllCountriesFile()
    {
        LoadGeoNameIds();
        var allFeatureCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var fc in _targetedFeatureCodesAdmin)
            allFeatureCodes.Add(fc);
        foreach (var fc in _targetedFeatureCodesCountry)
            allFeatureCodes.Add(fc);
        foreach (var fc in _targetedFeatureCodesCity)
            allFeatureCodes.Add(fc);

        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertEntity]");
        command.CommandType = CommandType.StoredProcedure;
        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 200);
        var asciiNameParam = command.Parameters.Add("@AsciiName", SqlDbType.NVarChar, 200);
        var alternateNamesParam = command.Parameters.Add("@AlternateNames", SqlDbType.NText);
        var latitudeParam = command.Parameters.Add("@Latitude", SqlDbType.Float);
        var longitudeParam = command.Parameters.Add("@Longitude", SqlDbType.Float);
        var featureClassParam = command.Parameters.Add("@FeatureClass", SqlDbType.NChar);
        var featureCodeParam = command.Parameters.Add("@FeatureCode", SqlDbType.NVarChar, 10);
        var countryCodeParam = command.Parameters.Add("@CountryCode", SqlDbType.NVarChar, 2);
        var cc2Param = command.Parameters.Add("@CC2", SqlDbType.NVarChar, 200);
        var admin1CodeParam = command.Parameters.Add("@Admin1Code", SqlDbType.NVarChar, 20);
        var admin2CodeParam = command.Parameters.Add("@Admin2Code", SqlDbType.NVarChar, 80);
        var admin3CodeParam = command.Parameters.Add("@Admin3Code", SqlDbType.NVarChar, 20);
        var admin4CodeParam = command.Parameters.Add("@Admin4Code", SqlDbType.NVarChar, 20);
        var admin5CodeParam = command.Parameters.Add("@Admin5Code", SqlDbType.NVarChar, 20);
        var populationParam = command.Parameters.Add("@Population", SqlDbType.BigInt, 8);
        var elevationParam = command.Parameters.Add("@Elevation", SqlDbType.Int, 4);
        var demParam = command.Parameters.Add("@Dem", SqlDbType.NVarChar, 20);
        var timezoneParam = command.Parameters.Add("@Timezone", SqlDbType.NVarChar, 40);
        var modificationDateParam = command.Parameters.Add("@ModificationDate", SqlDbType.DateTime);
        var continentParam = command.Parameters.Add("@Continent", SqlDbType.NVarChar, 2);
        var lineNumberParam = command.Parameters.Add("@LineNumber", SqlDbType.Int);

        ImportFlatTextFile("admin1CodesASCII.txt", '\t', 4, (lineNumber, fields) =>
        {
            var geoNameId = long.Parse(fields[3]);
            if (_geoNameIds.Contains(geoNameId))
                return false;

            var code = fields[0];
            var index = code.IndexOf('.');
            var countryCode = code.Substring(0, index).Trim();
            var adm1 = code.Substring(index+1).Trim();
            var name = fields[1];
            var asciiName = fields[2];
            
            geoNameIdParam.Value = geoNameId;
            nameParam.Value = name;
            asciiNameParam.Value = asciiName;
            alternateNamesParam.Value = "";
            latitudeParam.Value = 0.0;
            longitudeParam.Value = 0.0;
            featureClassParam.Value = 'A';
            featureCodeParam.Value = "ADM1";
            countryCodeParam.Value = countryCode;
            cc2Param.Value = "";
            admin1CodeParam.Value = adm1;
            admin2CodeParam.Value = "";
            admin3CodeParam.Value = "";
            admin4CodeParam.Value = "";
            admin5CodeParam.Value = "";
            populationParam.Value = 0L;
            elevationParam.Value = 0;
            demParam.Value = "";
            timezoneParam.Value = "";
            modificationDateParam.Value = _creationDate;
            continentParam.Value = "";
            lineNumberParam.Value = lineNumber;
            var result = Connection.ExecuteNonQuery(command, false);
            Debug.Assert(result > 0);
            _geoNameIds.Add(geoNameId);
            return true;
        });

        ImportFlatTextFile("admin2Codes.txt", '\t', 4, (lineNumber, fields) =>
        {
            var geoNameId = long.Parse(fields[3]);
            if (_geoNameIds.Contains(geoNameId))
                return false;

            var code = fields[0];
            var index1 = code.IndexOf('.');
            var index2 = code.IndexOf('.', index1+1);
            var countryCode = code.Substring(0, index1).Trim();
            var adm1 = code.Substring(index1+1, index2-index1-1).Trim();
            var adm2 = code.Substring(index2+1).Trim();
            var name = fields[1];
            var asciiName = fields[2];
            
            geoNameIdParam.Value = geoNameId;
            nameParam.Value = name;
            asciiNameParam.Value = asciiName;
            alternateNamesParam.Value = "";
            latitudeParam.Value = 0.0;
            longitudeParam.Value = 0.0;
            featureClassParam.Value = 'A';
            featureCodeParam.Value = "ADM2";
            countryCodeParam.Value = countryCode;
            cc2Param.Value = "";
            admin1CodeParam.Value = adm1;
            admin2CodeParam.Value = adm2;
            admin3CodeParam.Value = "";
            admin4CodeParam.Value = "";
            admin5CodeParam.Value = "";
            populationParam.Value = 0L;
            elevationParam.Value = 0;
            demParam.Value = "";
            timezoneParam.Value = "";
            modificationDateParam.Value = _creationDate;
            continentParam.Value = "";
            lineNumberParam.Value = lineNumber;
            var result = Connection.ExecuteNonQuery(command, false);
            Debug.Assert(result > 0);
            _geoNameIds.Add(geoNameId);
            return true;
        });


        ImportFlatTextFile("cities500.txt", '\t', 19, (lineNumber, fields) =>
        {
            var geoNameId = long.Parse(fields[0]);
            if (_geoNameIds.Contains(geoNameId))
                return false;
            if (!_targetedFeatureCodesCity.Contains(fields[7]))
                return false;
            
            geoNameIdParam.Value = geoNameId;
            var i = 1;
            nameParam.Value = fields[i++];
            asciiNameParam.Value = fields[i++];
            alternateNamesParam.Value = fields[i++];
            latitudeParam.Value = double.Parse(fields[i++]);
            longitudeParam.Value = double.Parse(fields[i++]);
            var featureClass = fields[i++];
            featureClassParam.Value = (featureClass.Length == 0) ? ' ' : featureClass[0];
            featureCodeParam.Value = fields[i++];
            countryCodeParam.Value = fields[i++];
            cc2Param.Value = fields[i++];
            admin1CodeParam.Value = fields[i++];
            admin2CodeParam.Value = fields[i++];
            admin3CodeParam.Value = fields[i++];
            admin4CodeParam.Value = fields[i++];
            admin5CodeParam.Value = "";
            var pop = long.Parse(fields[i++]);
            populationParam.Value = (pop < 500) ? 500 : pop;
            elevationParam.Value = int.TryParse(fields[i++], out var elevation) ? elevation : 0;
            demParam.Value = fields[i++];
            timezoneParam.Value = fields[i++];
            modificationDateParam.Value = DateTime.TryParse(fields[i++], out var md) ? md : _creationDate;
            continentParam.Value = "";
            lineNumberParam.Value = lineNumber;
            var result = Connection.ExecuteNonQuery(command, false);
            Debug.Assert(result > 0);
            _geoNameIds.Add(geoNameId);
            return true;
        });

    }
    

    private void ImportFromCitiesFile()
    {
        ImportFlatTextFile("cities500\\cities500.txt", '\t', 19, (lineNumber, fields) =>
        {
            if (!_targetedFeatureCodesCity.Contains(fields[7]))
                return false;

            var geoNameId = long.Parse(fields[0]);
            var population = long.Parse(fields[14]);
            if (population < 500)
                population = 500;

            var result = Connection.ExecuteNonQuery(
                $"""
                    UPDATE [{_geoNamesDatabase}].[dbo].[Entity]
                        SET [Population] = {population}
                        WHERE [GeoNameId] = {geoNameId}
                 """, false);

            if (result < 1)
            {
                Log.WriteLine($"Population not updated for {geoNameId}");
                return false;
            }

            return true;
        });
    }

    private void ImportFromCountryInfoFile()
    {
        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertEntity]");
        command.CommandType = CommandType.StoredProcedure;
        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 200);
        var asciiNameParam = command.Parameters.Add("@AsciiName", SqlDbType.NVarChar, 200);
        var alternateNamesParam = command.Parameters.Add("@AlternateNames", SqlDbType.NText);
        var latitudeParam = command.Parameters.Add("@Latitude", SqlDbType.Float);
        var longitudeParam = command.Parameters.Add("@Longitude", SqlDbType.Float);
        var featureClassParam = command.Parameters.Add("@FeatureClass", SqlDbType.NChar);
        var featureCodeParam = command.Parameters.Add("@FeatureCode", SqlDbType.NVarChar, 10);
        var countryCodeParam = command.Parameters.Add("@CountryCode", SqlDbType.NVarChar, 2);
        var cc2Param = command.Parameters.Add("@CC2", SqlDbType.NVarChar, 200);
        var admin1CodeParam = command.Parameters.Add("@Admin1Code", SqlDbType.NVarChar, 20);
        var admin2CodeParam = command.Parameters.Add("@Admin2Code", SqlDbType.NVarChar, 80);
        var admin3CodeParam = command.Parameters.Add("@Admin3Code", SqlDbType.NVarChar, 20);
        var admin4CodeParam = command.Parameters.Add("@Admin4Code", SqlDbType.NVarChar, 20);
        var admin5CodeParam = command.Parameters.Add("@Admin5Code", SqlDbType.NVarChar, 20);
        var populationParam = command.Parameters.Add("@Population", SqlDbType.BigInt, 8);
        var elevationParam = command.Parameters.Add("@Elevation", SqlDbType.Int, 4);
        var demParam = command.Parameters.Add("@Dem", SqlDbType.NVarChar, 20);
        var timezoneParam = command.Parameters.Add("@Timezone", SqlDbType.NVarChar, 40);
        var modificationDateParam = command.Parameters.Add("@ModificationDate", SqlDbType.DateTime);
        var continentParam = command.Parameters.Add("@Continent", SqlDbType.NVarChar, 2);
        var lineNumberParam = command.Parameters.Add("@LineNumber", SqlDbType.Int);

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
            var geoNameId = long.Parse(fields[i++]);
            var neighbours = fields[i++];
            var equivalentFipsCode = fields[i++];

            if (!_geoNameIds.Contains(geoNameId))
            {
                geoNameIdParam.Value = geoNameId;
                nameParam.Value = country;
                asciiNameParam.Value = country;
                alternateNamesParam.Value = "";
                latitudeParam.Value = 0.0;
                longitudeParam.Value = 0.0;
                featureClassParam.Value = 'A';
                featureCodeParam.Value = "COUNTRY";
                countryCodeParam.Value = iso;
                cc2Param.Value = iso3;
                admin1CodeParam.Value = "";
                admin2CodeParam.Value = "";
                admin3CodeParam.Value = "";
                admin4CodeParam.Value = "";
                admin5CodeParam.Value = "";
                populationParam.Value = long.Parse(population);
                elevationParam.Value = 0;
                demParam.Value = "";
                timezoneParam.Value = "";
                modificationDateParam.Value = _creationDate;
                continentParam.Value = continent;
                lineNumberParam.Value = lineNumber;
                var result1 = command.ExecuteNonQuery();
                Debug.Assert(result1 > 0);
                _geoNameIds.Add(geoNameId);
            }

            InsertGeoNamesAlternateName(geoNameId, country, lineNumber);
            InsertGeoNamesAlternateName(geoNameId, iso, lineNumber);
            InsertGeoNamesAlternateName(geoNameId, iso3, lineNumber);

        });
    }

    private void ImportAlternateNamesFile()
    {
        LoadGeoNameIds();
        ImportFlatTextFile("alternateNamesV2\\alternateNamesV2.txt", '\t', 10, (lineNumber, fields) =>
        {
            var i = 0;
            var alternateNameId = long.Parse(fields[i++]);
            var geonameId = long.Parse(fields[i++]);
            if (_geoNameIds.Contains(geonameId))
            {
                var language = fields[i++];
                if (language is "he" or "ru" or "ar" or "uk" or "zh" or "tt" 
                                        or "kk" or "fr_1793" or "fa" or "el" 
                                        or "xmf" or "bg" or "link" or "wkdt"
                                        or "post")
                    return false;

                var alternateName = fields[i++];
                if (TryTranslate(alternateName, out var translated))
                {
                    var isPreferredName = fields[i++] == "0";
                    var isShortName = fields[i++] == "0";
                    var isColloquial = fields[i++] == "0";
                    var isHistoric = fields[i++] == "0";

                    var from = ParseDateTime(fields[i++], MinDateTime);
                    var to = ParseDateTime(fields[i++], MaxDateTime);

                    if ((_creationDate >= from) && (_creationDate < to))
                    {
                        InsertGeoNamesAlternateName(alternateNameId, geonameId, language, translated!, isPreferredName, isShortName, isColloquial, isHistoric, from, to, lineNumber);
                        return true;
                    }
                }
            }
            return false;
        });
    }
    

    private void ImportFlatTextFile(string filename, char separator, int fieldCount, Action<int, string[]> process)
    {
        ImportFlatTextFile(filename, separator, fieldCount, (lineNumber, fields) =>
        {
            try
            {
                process(lineNumber, fields);
                return true;
            }
            catch (Exception e)
            {
                Error.WriteLine($"Exception while processing line {lineNumber}", e);
                return false;
            }
        });
    }


    private void ImportFlatTextFile(string filename, char separator, int fieldCount, Func<int, string[], bool> process)
    {
        // If a 'processed' file exists, use it instead.
        // If it doesn't exist, then create a 'temp' file and write to it.
        var fullFilename = Path.Combine(_workingFolder, filename);
        var input = File.OpenText(fullFilename);
        var lineNumber = 0;

        while (true)
        {
            var line = input.ReadLine();
            if (line == null)
                break;
            ++lineNumber;
            /*
            if (lineNumber < _line)
            {
                continue;
            }
            */
            if ((lineNumber % 1000) == 0)
                Log.WriteLine($"Line {lineNumber}").Flush();

            //_line = 0;
            if (line.StartsWith("#"))
            {
                continue;
            }

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

    private void ImportCountriesToContinentsFile()
    {
        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[AssignCountryToContinent]");
        command.CommandType = CommandType.StoredProcedure;
        var countryParam = command.Parameters.Add("@Country", SqlDbType.NChar, 2);
        var continentParam = command.Parameters.Add("@Continent", SqlDbType.NChar, 2);

        ImportFlatTextFile("..\\CountriesToContinents.txt", '\t', 3, (_, fields) =>
        {
            var iso = fields[0];
            var country = fields[1];
            var continent = fields[2];
            var bmContinent = _continentNameToAbbreviation[continent];


            countryParam.Value = iso;
            continentParam.Value = bmContinent;
            var result = Connection.ExecuteNonQuery(command, false);
            if (result < 0)
                Log.WriteLine($"Country {country} not assigned to continent {continent}");
         });
    }

    #endregion


    private void LoadGeoNameIds()
    {
        if (_geoNameIds.Count == 0)
            Connection.ExecuteReader($"SELECT GeoNameId FROM [{_geoNamesDatabase}].[dbo].[Entity]", r => _geoNameIds!.Add(r.GetInt64(0)), false);
    }


    private void InsertGeoNamesAlternateName(long altenativeNameId, long geonameId, string language, string name,
                    bool isPreferredName, bool isShortName, bool isColloguial, bool isHistoric,
                    DateTime from, DateTime to, int lineNumber)
    {
        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertAlternateName]", false);
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

        var result = Connection.ExecuteNonQuery(command, false);
        Debug.Assert(result > 0);
    }

    private void InsertGeoNamesAlternateName(long geonameId, string name, int lineNumber)
        => InsertGeoNamesAlternateName(0, geonameId, "", name, false, false, false, false, MinDateTime, MaxDateTime, lineNumber);

    
    private void LoadGeoNameCountries()
    {
        if (_geoNameCountryCodesToIds.Count > 0)
            return;

        Connection.ExecuteReader(
            $"""
                SELECT [GeoNameId], [CountryCode], [BenchmarkId]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = 'COUNTRY'
             """, 
            r =>
            {
                var id = r.GetInt64(0);
                var cc = r.GetString(1);
                var bm = r.GetInt64(2);

                if (string.IsNullOrWhiteSpace(cc))
                {
                    Log.WriteLine($"Invalid country code for {id}");
                }
                else
                {
                    _geoNameCountryCodesToIds.Add(cc, id);
                    _geoNameCountryIdsToBenchmarkIds.Add(id, bm);
                }
            });
    }
  
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
}