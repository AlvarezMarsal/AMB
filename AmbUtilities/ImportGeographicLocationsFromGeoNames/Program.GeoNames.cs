using System.Data;
using AmbHelper;
using System.Globalization;
using static AmbHelper.Logs;
using System.Diagnostics;

namespace ImportGeographicLocationsFromGeoNames;

internal partial class Program
{
    private void LoadGeoNameIds()
    {
        if (_geoNameIds.Count == 0)
            ConnectionG.ExecuteReader("SELECT GeoNameId FROM [GeoNames].[dbo].[Entity]", r => _geoNameIds!.Add(r.GetInt64(0)), false);
    }


    private void InsertGeoNamesAlternateName(long altenativeNameId, long geonameId, string language, string name,
                    bool isPreferredName, bool isShortName, bool isColloguial, bool isHistoric,
                    DateTime from, DateTime to, int lineNumber)
    {
        using var command = ConnectionG.CreateCommand("[GeoNames].[dbo].[InsertAlternateName]", false);
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

        var result = ConnectionG.ExecuteNonQuery(command, false);
        Debug.Assert(result > 0);
    }

    private void InsertGeoNamesAlternateName(long geonameId, string name, int lineNumber)
        => InsertGeoNamesAlternateName(0, geonameId, "", name, false, false, false, false, MinDateTime, MaxDateTime, lineNumber);

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

    #region Import From GeoNames

    private void ImportFromAllCountriesFile()
    {
        LoadGeoNameIds();

        ImportFlatTextFile("allcountries\\allcountries.txt", '\t', 19, (lineNumber, fields) =>
        {
            var geoNameId = long.Parse(fields[0]);
            if (_geoNameIds.Contains(geoNameId))
                return;

            if (fields[6].Length != 1) // No feature class
            {
                Error.WriteLine($"No feature class on line {lineNumber} for {fields[2]}");
                return;
            }
            var featureClass = fields[6][0];
            if (featureClass is not ('A' or 'P'))
                return;

            using var command = ConnectionG.CreateCommand("[GeoNames].[dbo].[InsertEntity]");
            command.CommandType = CommandType.StoredProcedure;
            command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8).Value = geoNameId;
            var i = 1;
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
            command.Parameters.Add("@ModificationDate", SqlDbType.DateTime).Value = DateTime.TryParse(fields[i++], out var md) ? md : _creationDate;
            command.Parameters.Add("@Continent", SqlDbType.NVarChar, 2).Value = "";
            command.Parameters.Add("@LineNumber", SqlDbType.Int).Value = lineNumber;
            var result = command.ExecuteNonQuery();
            Debug.Assert(result > 0);
            _geoNameIds.Add(geoNameId);
        });
    }

    private void ImportFromCitiesFile()
    {
        ImportFlatTextFile("cities500\\cities500.txt", '\t', 19, (lineNumber, fields) =>
        {
            if (fields[6].Length != 1) // No feature class
                Log.WriteLine("No feature class on line {lineNumber}");
            var featureClass = fields[6][0];
            if (featureClass is not ('A' or 'P'))
                return;

            var geoNameId = long.Parse(fields[0]);
            var population = long.Parse(fields[14]);
            if (population < 500)
                population = 500;

            var result = ConnectionG.ExecuteNonQuery(
                $"""
                    UPDATE [GeoNames].[dbo].[Entity]
                        SET [Population] = {population}
                        WHERE [GeoNameId] = {geoNameId}
                 """, false);

            if (result < 1)
                Log.WriteLine($"Population not updated for {geoNameId}");
        });
    }

    /*
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
    */

    private void ImportFromCountryInfoFile()
    {
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

            InsertGeoNamesAlternateName(geonameId, country, lineNumber);
            InsertGeoNamesAlternateName(geonameId, iso, lineNumber);
            InsertGeoNamesAlternateName(geonameId, iso3, lineNumber);
            InsertGeoNamesAlternateName(geonameId, country, lineNumber);

            var result = ConnectionG.ExecuteNonQuery(
                $"""
                    UPDATE [dbo].[Entity]
                        SET [Continent] = N'{continent}'
                        WHERE [GeoNameId] = {geonameId} -- OR CountryCode = N'{iso}'
                 """);
        });
    }

    private void ImportAlternateNamesFile()
    {
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

                    if ((_creationDate >= from) && (_creationDate < to))
                        InsertGeoNamesAlternateName(0, geonameId, language, translated!, isPreferredName, isShortName, isColloquial, isHistoric, from, to, lineNumber);
                }
            }
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
                Log.WriteLine($"Line {lineNumber}").Flush();
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

    private void ImportCountriesToContinentsFile()
    {
        ImportFlatTextFile("..\\CountriesToContinents.txt", '\t', 3, (_, fields) =>
        {
            var iso = fields[0];
            var country = fields[1];
            var continent = fields[2];
            var bmContinent = _continentNameToAbbreviation[continent];
            var result = ConnectionG.ExecuteNonQuery(
                $"""
                    UPDATE [dbo].[Entity]
                        SET [Continent] = N'{bmContinent}'
                        WHERE CountryCode = N'{iso}'
                 """);
        });
    }

    #endregion
    
    private void LoadGeoNameCountries()
    {
        if (_geoNameCountryCodesToIds.Count > 0)
            return;

        ConnectionG.ExecuteReader(
            $"""
                SELECT [GeoNameId], [CountryCode], [BenchmarkId]
                FROM [dbo].[Entity]
                WHERE [FeatureCode] IN 
                    (
                        --'LTER', --	leased area	a tract of land leased to another country, usually for military installations
                        'PCL', --	political entity	
                        'PCLD', --	dependent political entity	
                        'PCLF', --	freely associated state	
                        --'PCLH', --	historical political entity	a former political entity
                        'PCLI', --	independent political entity	
                        'PCLIX', --	section of independent political entity	
                        'PCLS', --	semi-independent political entity	
                        --'PRSH', --	parish	an ecclesiastical district
                        'TERR' --	territory	
                    )
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