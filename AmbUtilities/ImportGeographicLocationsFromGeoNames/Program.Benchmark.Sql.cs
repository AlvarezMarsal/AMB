using System.Security.Cryptography;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace ImportGeographicLocationsFromGeoNames;

internal record class Country(long GeoNameId, string AsciiName, string CountryCode, string Continent);

internal partial class Program
{
    private void WriteImportSql()
    {
        foreach (var filename in Directory.GetFiles(".", "ImportGeographicLocations-*.sql"))
            File.Delete(filename);

        using (var sql = File.CreateText("ImportGeographicLocations-1.sql"))
        {
            sql.WriteLine("USE [AMBenchmark_DB]");
            sql.WriteLine("GO");
            WriteContinentsSql(sql);
            sql.Flush();
        }

        using (var sql = File.CreateText("ImportGeographicLocations-2.sql"))
        {
            sql.WriteLine("USE [AMBenchmark_DB]");
            sql.WriteLine("GO");
            WriteCountriesSql(sql);
            sql.Flush();
        }

        using (var sql = File.CreateText("ImportGeographicLocations-3.sql"))
        {
            sql.WriteLine("USE [AMBenchmark_DB]");
            sql.WriteLine("GO");
            WriteStatesSql(sql);
            sql.Flush();
        }

        using (var sql = File.CreateText("ImportGeographicLocations-4A.sql"))
        {
            using (var sql2 = File.CreateText("ImportGeographicLocations-4B.sql"))
            {
                sql.WriteLine("USE [AMBenchmark_DB]");
                sql.WriteLine("GO");
                sql2.WriteLine("USE [AMBenchmark_DB]");
                sql2.WriteLine("GO");
                WriteCountiesSql([sql, sql2], cc => (string.Compare(cc, "MA") < 0) ? 0 : 1);
                sql.Flush();
            }
        }

        using (var sql = File.CreateText("ImportGeographicLocations-5A.sql"))
        {
            using (var sql2 = File.CreateText("ImportGeographicLocations-5B.sql"))
            {
                sql.WriteLine("USE [AMBenchmark_DB]");
                sql.WriteLine("GO");
                sql2.WriteLine("USE [AMBenchmark_DB]");
                sql2.WriteLine("GO");
                WriteCitiesSql([sql, sql2]);
                sql.Flush();
            }
        }

        using (var sql = File.CreateText("ImportGeographicLocations-6.sql"))
        {
            sql.WriteLine("USE [AMBenchmark_DB]");
            sql.WriteLine("GO");
            WriteAliasesSql(sql);
            sql.Flush();
        }
    }

    private void WriteLocation(StreamWriter sql, long pid, string name)
        => WriteLocation(sql, pid.ToString(), name);

    private void WriteLocation(StreamWriter sql, string pid, string name)
    {
        name = name.Replace("'", "''");
        sql.WriteLine($"""
                        IF NOT EXISTS (SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {pid} AND [Name] = N'{name}')
                        BEGIN
                            DECLARE @oid1 BIGINT
                            EXECUTE [dbo].[sp_internalGetNextOid] @oid1 OUTPUT
                            DECLARE @index INT
                            SELECT @index=MAX([Index]) FROM [dbo].[t_GeographicLocation] WHERE [PID] = {pid}
                            IF @index IS NULL
                                SET @index = 0
                            ELSE
                                SET @index = @index + 1
                            INSERT INTO [dbo].[t_GeographicLocation] 
                                ([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorId], [PracticeAreaID], [CreationSession])
                            VALUES
                                (@oid1, {pid}, 1, N'{name}', @index, N'{name}', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')
                        END
                        """);
    }


    private void WriteAlias(StreamWriter sql, long pid, string name, string alias)
        => WriteAlias(sql, pid.ToString(), name, alias);

    private void WriteAlias(StreamWriter sql, string pid, string name, string alias)
    {
        name = name.Replace("'", "''");
        alias = alias.Replace("'", "''");
        sql.WriteLine($"""
                        DECLARE @oid2 BIGINT
                        SELECT @oid2 = [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {pid} AND [Name] = N'{name}'
                        IF (@oid2 IS NOT NULL)
                        BEGIN
                            IF NOT EXISTS (SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationId] = @oid2 AND [Alias] = N'{alias}')
                            BEGIN
                                DECLARE @aliasOid BIGINT
                                DECLARE @isPrimary BIT
                                EXECUTE [dbo].[sp_internalGetNextOid] @aliasOid OUTPUT
                                SELECT @isPrimary = CASE WHEN EXISTS (SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationId] = @oid2 AND [IsPrimary] = 1) THEN 0 ELSE 1 END
                                INSERT INTO [dbo].[t_GeographicLocationAlias] 
                                    ([OID],[Alias],[Description],[IsSystemOwned],[IsPrimary],[CreationDate],[CreatorId],[PracticeAreaID],[GeographicLocationID],[LID],[CreationSession])
                                VALUES
                                    (@aliasOid, '{alias}', '{name}', 1, @isPrimary, '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, @oid2, 500,'{_creationSessionAsString}')
                            END
                        END
                        """);
    }

    private void WriteAlias(StreamWriter sql, long oid, string alias)
    {
        alias = alias.Replace("'", "''");
        sql.WriteLine($"""
                        IF NOT EXISTS (SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationId] = {oid} AND [Alias] = N'{alias}')");
                        BEGIN
                            DECLARE @asciiName NVARCHAR(200)
                            DECLARE @aliasOid BIGINT
                            DECLARE @isPrimary BIT
                            SELECT @asciiName = [Name] FROM [dbo].[t_GeographicLocation] WHERE [OID] = {oid}
                            EXECUTE [dbo].[sp_internalGetNextOid] @aliasOid OUTPUT
                            SELECT @isPrimary = CASE WHEN EXISTS (SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationId] = @oid AND [IsPrimary] = 1) THEN 0 ELSE 1 END
                            INSERT INTO [dbo].[t_GeographicLocationAlias] 
                                ([OID],[Alias],[Description],[IsSystemOwned],[IsPrimary],[CreationDate],[CreatorId],[PracticeAreaID],[GeographicLocationID],[LID],[CreationSession])
                            VALUES
                                (@oid, '{alias}', @asciiName, 1, @isPrimary, '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, @oid, 500,'{_creationSessionAsString}')
                        END
                        """);
    }

    private void WriteGetCountryOid(StreamWriter sql, string countryName, string variableName)
    {
        countryName = countryName.Replace("'", "''");
        sql.WriteLine($"""
                        DECLARE {variableName} BIGINT
                        SELECT {variableName} = Country.[OID] 
                            FROM [dbo].[t_GeographicLocation] Country 
                            JOIN [dbo].[t_GeographicLocation] Continent ON (Continent.OID = Country.PID)
                            WHERE Country.[Name] = N'{countryName}' AND Continent.[PID] = {_worldId}
                        """);
    }

    private void WriteGetStateOid(StreamWriter sql, string countryOid, string stateName, string variableName)
    {
        stateName = stateName.Replace("'", "''");
        sql.WriteLine($"""
                        DECLARE {variableName} BIGINT
                        SELECT {variableName} = State.[OID] 
                            FROM [dbo].[t_GeographicLocation] State 
                            JOIN [dbo].[t_GeographicLocation] Country ON (Country.OID = State.PID)
                            WHERE State.[Name] = N'{stateName}' AND Country.[OID] = {countryOid}
                        """);
    }

    private void WriteGetCountyOid(StreamWriter sql, string countryOid, string stateOid, string countyName, string variableName)
    {
        countyName = countyName.Replace("'", "''");
        sql.WriteLine($"""
                        DECLARE {variableName} BIGINT
                        SELECT {variableName} = County.[OID] 
                            FROM [dbo].[t_GeographicLocation] County 
                            FROM [dbo].[t_GeographicLocation] State ON (State.OID = County.PID)
                            JOIN [dbo].[t_GeographicLocation] Country ON (Country.OID = State.PID)
                            WHERE County.[Name] = N'{countyName}' AND Country.[OID] = {countryOid}
                        """);
    }

    private void WriteContinentsSql(StreamWriter sql)
    {
        // Get the world
        sql.WriteLine("-- ASSUME WORLD EXISTS");
        foreach (var continent in _continentNameToAbbreviation)
        {
            sql.WriteLine($"PRINT N'CREATE CONTINENT {continent}'");
            WriteLocation(sql, _worldId, continent.Key);
            WriteAlias(sql, _worldId, continent.Key, continent.Key);
            sql.WriteLine("GO");
        }
    }

    private void WriteCountriesSql(StreamWriter sql)
    {
        var countries = Connection.Select(
            $"""
            SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
            FROM [{_geoNamesDatabase}].[dbo].[Entity]
            WHERE [FeatureCode] = N'COUNTRY'
            """,
            (r) => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), CountryCode = r.GetString(2), Continent = r.GetString(3), BenchmarkId = r.GetInt64(4) },
            false);

        foreach (var country in countries)
        {
            var abbreviation = _continentAbbreviationToName[country.Continent];
            sql.WriteLine($"-- CREATE COUNTRY {country.AsciiName}");
            sql.WriteLine($"DECLARE @Continent BIGINT");
            sql.WriteLine($"SELECT @Continent = [OID] FROM [dbo].[t_GeographicLocation] WHERE Name=N'{abbreviation}' AND PID={_worldId}");
            WriteLocation(sql, "@Continent", country.AsciiName);
            WriteAlias(sql, "@Continent", country.AsciiName, country.AsciiName);
            sql.WriteLine("GO");
        }
    }


    private void WriteStatesSql(StreamWriter sql)
    {
        var countries = Connection.Select(
            $"""
            SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
            FROM [{_geoNamesDatabase}].[dbo].[Entity]
            WHERE [FeatureCode] = N'COUNTRY'
            """,
            (r) => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), CountryCode = r.GetString(2), Continent = r.GetString(3), BenchmarkId = r.GetInt64(4) },
            false);

        foreach (var country in countries)
        {
            sql.WriteLine($"--CREATNG STATES OF {country.AsciiName}");
            var states = Connection.Select(
                    $"""
                        SELECT [GeoNameId], [AsciiName], [BenchmarkId]
                        FROM [{_geoNamesDatabase}].[dbo].[Entity]
                        WHERE [FeatureCode] = N'ADM1' AND [CountryCode] = N'{country.CountryCode}'
                     """,
                    r => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), BenchmarkId = r.GetInt64(2) },
                    false);

            foreach (var state in states)
            {
                var s = state.AsciiName.Replace("'", "''");
                var c = country.AsciiName.Replace("'", "''");
                sql.WriteLine($"PRINT N'CREATING STATE {s} OF {c}'");
                WriteGetCountryOid(sql, country.AsciiName, "@countryOid");
                WriteLocation(sql, "@countryOid", state.AsciiName);
                WriteAlias(sql, "@countryOid", state.AsciiName, state.AsciiName);
                sql.WriteLine("GO");
            }
        }
    }

    private void WriteCountiesSql(StreamWriter[] sqls, Func<string, int> chooser)
    {
        var countries = Connection.Select(
            $"""
            SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
            FROM [{_geoNamesDatabase}].[dbo].[Entity]
            WHERE [FeatureCode] = N'COUNTRY'
            """,
            (r) => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), CountryCode = r.GetString(2), Continent = r.GetString(3), BenchmarkId = r.GetInt64(4) },
            false);

        foreach (var country in countries)
        {
            var sql = sqls[chooser(country.CountryCode)];

            var c = country.AsciiName.Replace("'", "''");
            sql.WriteLine($"PRINT N'CREATNG COUNTIES OF {c}'");

            var states = Connection.Select(
                    $"""
                        SELECT [GeoNameId], [AsciiName], [BenchmarkId], [Admin1Code]
                        FROM [{_geoNamesDatabase}].[dbo].[Entity]
                        WHERE [FeatureCode] = N'ADM1' AND [CountryCode] = N'{country.CountryCode}'
                     """,
                    r => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), BenchmarkId = r.GetInt64(2), Admin1Code = r.GetString(3) },
                    false);

            foreach (var state in states)
            {
                var a = state.Admin1Code.Replace("'", "''");
                var counties = Connection.Select(
                        $"""
                            SELECT [GeoNameId], [AsciiName], [BenchmarkId]
                            FROM [{_geoNamesDatabase}].[dbo].[Entity]
                            WHERE [FeatureCode] = N'ADM2' AND [CountryCode] = N'{country.CountryCode}' AND [Admin1Code] = N'{a}'
                         """,
                        r => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), BenchmarkId = r.GetInt64(2) },
                        false);

                if (counties.Count > 0)
                {
                    var s1 = state.AsciiName.Replace("'", "''");
                    var c1 = country.AsciiName.Replace("'", "''");
                    sql.WriteLine($"PRINT N'CREATNG COUNTIES OF STATE {s1} OF {c1}'");
                }

                foreach (var county in counties)
                {
                    sql.WriteLine($"--CREATNG COUNTY {county.AsciiName} OF STATE {state.AsciiName} OF {country.AsciiName}");
                    WriteGetCountryOid(sql, country.AsciiName, "@countryOid");
                    WriteGetStateOid(sql, "@countryOid", state.AsciiName, "@stateOid");
                    WriteLocation(sql, "@stateOid", county.AsciiName);
                    WriteAlias(sql, "@stateOid", county.AsciiName, county.AsciiName);
                    sql.WriteLine("GO");
                }

                sql.Flush();
            }
        }
    }

    private void WriteCitiesSql(StreamWriter[] sqls)
    {
        var countries = Connection.Select(
            $"""
            SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
            FROM [{_geoNamesDatabase}].[dbo].[Entity]
            WHERE [FeatureCode] = N'COUNTRY'
            """,
            (r) => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), CountryCode = r.GetString(2), Continent = r.GetString(3), BenchmarkId = r.GetInt64(4) },
            false);

        foreach (var country in countries)
        {
            var sql = sqls[country.GeoNameId % sqls.Length];
            var c = country.AsciiName.Replace("'", "''");
            sql.WriteLine($"PRINT N'CREATNG CITIES OF {c}'");

            var cities = Connection.Select(
                    $"""
                    SELECT City.[AsciiName],
                        ISNULL([State].[AsciiName], '')  As StateAsciiName,
                        ISNULL([County].[AsciiName], '')  AS CountyAsciiName
                    FROM [{_geoNamesDatabase}].[dbo].[Entity] City
                    LEFT OUTER JOIN [{_geoNamesDatabase}].[dbo].[Entity] State ON (State.FeatureCode = 'ADM1' AND State.CountryCode = City.CountryCode AND State.Admin1Code = City.Admin1Code)
                    LEFT OUTER JOIN [{_geoNamesDatabase}].[dbo].[Entity] County ON (County.FeatureCode = 'ADM2' AND County.CountryCode = City.CountryCode AND County.Admin1Code = City.Admin1Code AND County.Admin2Code = City.Admin2Code)
                    WHERE City.[FeatureCode] <> N'COUNTRY' AND City.[FeatureCode] NOT LIKE N'ADM%' AND City.CountryCode = N'{country.CountryCode}'
                 """,
                (r) => new { AsciiName = r.GetString(0), StateName = r.GetString(1), CountyName = r.GetString(2) },
                false);

            foreach (var city in cities)
            {
                WriteGetCountryOid(sql, country.AsciiName, "@countryOid");
                if (city.StateName == "")
                {
                    WriteLocation(sql, "@countryOid", city.AsciiName);
                    WriteAlias(sql, "@countryOid", city.AsciiName, city.AsciiName);
                    sql.WriteLine("GO");
                    continue;
                }
                WriteGetStateOid(sql, "@countryOid", city.StateName, "@stateOid");
                if (city.CountyName == "")
                {
                    WriteLocation(sql, "@stateOid", city.AsciiName);
                    WriteAlias(sql, "@stateOid", city.AsciiName, city.AsciiName);
                    sql.WriteLine("GO");
                    continue;
                }
                WriteGetCountyOid(sql, "@countryOid", "@stateOid", city.CountyName, "@countyOid");
                WriteLocation(sql, "@countyOid", city.AsciiName);
                WriteAlias(sql, "@countyOid", city.AsciiName, city.AsciiName);
                sql.WriteLine("GO");
            }
        }
    }

    private void WriteAliasesSql(StreamWriter sql)
    {
        var countries = Connection.Select(
             $"""
            SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
            FROM [{_geoNamesDatabase}].[dbo].[Entity]
            WHERE [FeatureCode] = N'COUNTRY'
            """,
             (r) => new { GeoNameId = r.GetInt64(0), AsciiName = r.GetString(1), CountryCode = r.GetString(2), Continent = r.GetString(3), BenchmarkId = r.GetInt64(4) },
             false);

        foreach (var country in countries)
        {
            sql.WriteLine($"--CREATNG ALIASES OF {country.AsciiName}");
            var aliases = Connection.Select(
                $"""
                    SELECT E.[GeoNameId], A.[AlternateName]
                    FROM [{_geoNamesDatabase}].[dbo].[AlternateName] A
                    JOIN [{_geoNamesDatabase}].[dbo].[Entity] E ON (E.GeoNameId = A.GeoNameId)
                    WHERE E.[CountryCode] = N'{country.CountryCode}'
                 """,
                (r) => new { BenchmarkId = r.GetInt64(0), Name = r.GetString(1) },
                false);

            foreach (var alias in aliases)
            {
                WriteAlias(sql, alias.BenchmarkId, alias.Name);
                sql.WriteLine("GO");
            }
        }
    }
}
