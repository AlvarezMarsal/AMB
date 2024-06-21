using System.Diagnostics;
using System.Data;
using AmbHelper;
using System.Globalization;


namespace ImportGeographyFromGeoNames;

internal partial class Program
{
    private static class FieldIndex
    {
        public const int Id = 0;
        public const int Name = 1;
        public const int AsciiName = 2;
        public const int AlternateNames = 3;            // var alternatenames = fields[i++].Split(',');    // alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
        //i++;                                          // latitude: latitude in decimal degrees(wgs84)
        //i++;                                          // longitude: longitude in decimal degrees(wgs84)
        public const int FeatureClass = 6;              
        public const int FeatureCode = 7;
        public const int CountryCode = 8;                   // ISO-3166 2-letter country code, 2 characters
        public const int AlternateCountryCodes = 9;        // alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
        public const int Admin1 = 10;                       // fipscode(subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
        public const int Admin2 = 11;                       // var admin2code = fields[i++].Trim();             // code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
        public const int Admin3 = 12;                       // admin3 code       : code for third level administrative division, varchar(20)
        public const int Admin4 = 13;                       // admin4 code       : code for fourth level administrative division, varchar(20)
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

}