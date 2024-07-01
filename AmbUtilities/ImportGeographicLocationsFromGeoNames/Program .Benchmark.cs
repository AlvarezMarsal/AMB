using System.Data;
using static AmbHelper.Logs;

namespace ImportGeographicLocationsFromGeoNames;

internal partial class Program
{
    #region Import Continents

    private void ImportContinents()
    {
        // Get the world
        var w = Connection.SelectOneValue("SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] IS NULL", (r) => r.GetInt64(0));
        if (!w.HasValue)
        {
            _worldId = 20000;
            Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                "([OID],[PID],[IsSystemOwned],[Name],[Index],[Description],[CreationDate],[CreatorId],[PracticeAreaID],[CreationSession]) " +
                "VALUES " +
                $"({_worldId}, NULL, 1, N'World', 0, N'World', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')", 
                false);
            AddAliasToDatabase(_worldId, "World");
        }
        else
        {
            _worldId = w.Value;
        }

        // Create any continents that don't already exist
        LoadExistingContinentIds();
        foreach (var kvp in _continentNameToAbbreviation)
        {
            if (_continentAbbreviationToId.ContainsKey(kvp.Value))
                continue;
            var index = NextChildIndex(_worldId);
            var id = Connection.GetNextOid();
            Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocation] " +
                "([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorId], [PracticeAreaID], [CreationSession]) " +
                "VALUES " +
                $"({id}, {_worldId}, 1, N'{kvp.Key}', {index}, N'{kvp.Key}', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')",
                false);
            _continentAbbreviationToId.Add(kvp.Value, id);
            AddAliasToDatabase(id, kvp.Key, kvp.Key);
        }
    }
    
    private void LoadExistingContinentIds()
    {
        if (_continentAbbreviationToId.Count > 0)
            return;
        foreach (var kvp in _continentNameToAbbreviation)
        {
            var id = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {_worldId} AND [Name] = N'{kvp.Key}'",
                                               (r) => r.GetInt64(0),
                                               false);
            if (id.HasValue)
            {
                _continentAbbreviationToId.Add(kvp.Value, id.Value);
                AddAliasToDatabase(id.Value, kvp.Key, kvp.Key);
            }
        }
    }

    #endregion

    #region Import Countries

    private void ImportCountries()
    {
        LoadExistingContinentIds();

        var countries = Connection.Select(
            $"""
                SELECT [GeoNameId], [AsciiName], [CountryCode], [Continent], [BenchmarkId]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = N'COUNTRY'
             """, 
            (r) => new { GeoNameId=r.GetInt64(0), AsciiName=r.GetString(1), CountryCode=r.GetString(2), Continent=r.GetString(3), BenchmarkId=r.GetInt64(4)}, 
            false);

        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertGeographicLocation]", false);
        command.CommandType = CommandType.StoredProcedure;

        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var benchmarkIdParam = command.Parameters.Add("@BenchmarkId", SqlDbType.BigInt, 8);
        benchmarkIdParam.Direction = ParameterDirection.InputOutput;
        var parentIdParam = command.Parameters.Add("@ParentId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 512);
        command.Parameters.Add("@PracticeAreaId", SqlDbType.Int).Value = _practiceAreaId;
        command.Parameters.Add("@CreationDate", SqlDbType.DateTime2).Value = _creationDate;
        command.Parameters.Add("@CreatorId", SqlDbType.Int).Value = _creatorId;
        command.Parameters.Add("@CreationSession", SqlDbType.UniqueIdentifier).Value = _creationSession;

        foreach (var country in countries)
        {
            try
            {
                var continentId = _continentAbbreviationToId[country.Continent];

                geoNameIdParam.Value = country.GeoNameId;
                benchmarkIdParam.Value = country.BenchmarkId;
                parentIdParam.Value = continentId;
                nameParam.Value = country.AsciiName;

                var result = Connection.ExecuteNonQuery(command, false);
                if (result < 0)
                    throw new Exception($"Error inserting country {country.AsciiName}");
            }
            catch (Exception e)
            {
                Error.WriteLine(e);
            }
        } 
    }
        
    private void LoadExistingCountryIds()
    {
        if (_countryIds.Count > 0)
            return;
        var c = string.Join(",", _continentAbbreviationToId.Values);
        var d = Connection.Select(
            $"""
                SELECT [OID]
                FROM [dbo].[t_GeographicLocation]
                WHERE [PID] IN ({c})
             """, 
            (r) => r.GetInt64(0), 
            false);
        foreach (var  e in d)
            _countryIds.Add(e);
    }

    #endregion

    #region Import States

    private void ImportStates()
    {
        var countryCodesToBenchmarkIds = Connection.Select(
            $"""
                SELECT [CountryCode], [BenchmarkId]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = N'COUNTRY'
             """, 
            (r) => new KeyValuePair<string, long>(r.GetString(0), r.GetInt64(1)), 
            false);

        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertGeographicLocation]", false);
        command.CommandType = CommandType.StoredProcedure;

        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var benchmarkIdParam = command.Parameters.Add("@BenchmarkId", SqlDbType.BigInt, 8);
        benchmarkIdParam.Direction = ParameterDirection.InputOutput;
        var parentIdParam = command.Parameters.Add("@ParentId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 512);
        command.Parameters.Add("@PracticeAreaId", SqlDbType.Int).Value = _practiceAreaId;
        command.Parameters.Add("@CreationDate", SqlDbType.DateTime2).Value = _creationDate;
        command.Parameters.Add("@CreatorId", SqlDbType.Int).Value = _creatorId;
        command.Parameters.Add("@CreationSession", SqlDbType.UniqueIdentifier).Value = _creationSession;

 
        foreach (var kvp in countryCodesToBenchmarkIds)
        {
            Log.WriteLine($"Importing states of {kvp.Key} into Benchmark");
            var states = Connection.Select(
                $"""
                    SELECT [GeoNameId], [AsciiName], [BenchmarkId]
                    FROM [{_geoNamesDatabase}].[dbo].[Entity]
                    WHERE [FeatureCode] = N'ADM1' AND [CountryCode] = N'{kvp.Key}'
                 """, 
                r => new { GeoNameId=r.GetInt64(0), AsciiName=r.GetString(1), BenchmarkId=r.GetInt64(2)}, 
                false);

            foreach (var state in states)
            {
                try
                {
                    geoNameIdParam.Value = state.GeoNameId;
                    benchmarkIdParam.Value = state.BenchmarkId;
                    parentIdParam.Value = kvp.Value;
                    nameParam.Value = state.AsciiName;

                    var result = Connection.ExecuteNonQuery(command, false);
                    if (result < 0)
                        throw new Exception("Bad result from InsertGeographicLocation");
                }
                catch (Exception ex)
                {
                    Error.WriteLine($"Error inserting state {state.AsciiName}", ex);
                }
            }
        }
    }

    #endregion

    private void ImportCounties()
    {
         using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertGeographicLocation]", false);
        command.CommandType = CommandType.StoredProcedure;

        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var benchmarkIdParam = command.Parameters.Add("@BenchmarkId", SqlDbType.BigInt, 8);
        benchmarkIdParam.Direction = ParameterDirection.InputOutput;
        var parentIdParam = command.Parameters.Add("@ParentId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 512);
        command.Parameters.Add("@PracticeAreaId", SqlDbType.Int).Value = _practiceAreaId;
        command.Parameters.Add("@CreationDate", SqlDbType.DateTime2).Value = _creationDate;
        command.Parameters.Add("@CreatorId", SqlDbType.Int).Value = _creatorId;
        command.Parameters.Add("@CreationSession", SqlDbType.UniqueIdentifier).Value = _creationSession;

        var countryCodes = Connection.Select(
            $"""
                SELECT [CountryCode]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = N'COUNTRY'
             """, 
            (r) => r.GetString(0),
            false);

        foreach (var countryCode in countryCodes)
        {
            var states = Connection.Select(
                $"""
                    SELECT [Admin1Code], [BenchmarkId]
                    FROM [{_geoNamesDatabase}].[dbo].[Entity]
                    WHERE [FeatureCode] = N'ADM1' AND [CountryCode] = N'{countryCode}'
                 """, 
                r => new { Admin1Code=r.GetString(0), BenchmarkId=r.GetInt64(1)},
                false);

            foreach (var state in states)
            {
                Log.WriteLine($"Importing counties of {countryCode}.{state.Admin1Code} into Benchmark");
                var counties = Connection.Select(
                    $"""
                        SELECT [GeoNameId], [AsciiName], [BenchmarkId]
                        FROM [{_geoNamesDatabase}].[dbo].[Entity]
                        WHERE [FeatureCode] = N'ADM2' AND [CountryCode] = N'{countryCode}' AND [Admin1Code] = N'{state.Admin1Code}'
                     """, 
                    r => new { GeoNameId=r.GetInt64(0), AsciiName=r.GetString(1), BenchmarkId=r.GetInt64(2)},
                    false);

                foreach (var county in counties)
                {
                    try
                    {
                        geoNameIdParam.Value = county.GeoNameId;
                        benchmarkIdParam.Value = county.BenchmarkId;
                        parentIdParam.Value = state.BenchmarkId;
                        nameParam.Value = county.AsciiName;

                        var result = Connection.ExecuteNonQuery(command, false);
                        if (result < 0)
                            throw new Exception("Bad result from InsertGeographicLocation");
                    }
                    catch (Exception ex)
                    {
                        Error.WriteLine($"Error inserting county {county.AsciiName}", ex);
                    }
                }
            }
        }
    }

    private void ImportCities()
    {
        var countryCodesToBenchmarkIds = Connection.Select(
            $"""
                SELECT [CountryCode], [BenchmarkId]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = N'COUNTRY'
             """, 
            (r) => new KeyValuePair<string, long>(r.GetString(0), r.GetInt64(1)), 
            false);

        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertGeographicLocation]", false);
        command.CommandType = CommandType.StoredProcedure;

        var geoNameIdParam = command.Parameters.Add("@GeoNameId", SqlDbType.BigInt, 8);
        var benchmarkIdParam = command.Parameters.Add("@BenchmarkId", SqlDbType.BigInt, 8);
        benchmarkIdParam.Direction = ParameterDirection.InputOutput;
        var parentIdParam = command.Parameters.Add("@ParentId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 512);
        command.Parameters.Add("@PracticeAreaId", SqlDbType.Int).Value = _practiceAreaId;
        command.Parameters.Add("@CreationDate", SqlDbType.DateTime2).Value = _creationDate;
        command.Parameters.Add("@CreatorId", SqlDbType.Int).Value = _creatorId;
        command.Parameters.Add("@CreationSession", SqlDbType.UniqueIdentifier).Value = _creationSession;

        foreach (var country in countryCodesToBenchmarkIds)
        {
            Log.WriteLine($"Importing cities of {country.Key} into Benchmark");

            var cities = Connection.Select(
                $"""
                    SELECT C.[GeoNameId], C.[AsciiName], C.[BenchmarkId], /*C.[CountryCode], C.[Admin1Code], C.[Admin2Code],*/ ISNULL(S.[BenchmarkId],0) As [StateBenchmarkId], ISNULL(X.[BenchmarkId],0) As [CountyBenchmarkId]
                    FROM [{_geoNamesDatabase}].[dbo].[Entity] C
                    LEFT OUTER JOIN [{_geoNamesDatabase}].[dbo].[Entity] S ON (S.FeatureCode = 'ADM1' AND S.CountryCode = C.CountryCode AND S.Admin1Code = C.Admin1Code)
                    LEFT OUTER JOIN [{_geoNamesDatabase}].[dbo].[Entity] X ON (X.FeatureCode = 'ADM2' AND X.CountryCode = C.CountryCode AND X.Admin1Code = C.Admin1Code AND X.Admin2Code = C.Admin2Code)
                    WHERE C.[FeatureCode] <> N'COUNTRY' AND C.[FeatureCode] NOT LIKE N'ADM%' AND C.CountryCode = N'{country.Key}'
                 """, 
            (r) => new { GeoNameId=r.GetInt64(0), AsciiName=r.GetString(1), BenchmarkId=r.GetInt64(2), StateId=r.GetInt64(3), CountyId=r.GetInt64(4)},
            false);

            foreach (var  city in cities)
            {
                var parentId = city.CountyId;

                if (parentId == 0)
                {
                    parentId = city.StateId;
                    if (parentId == 0)
                        parentId = country.Value;
                }

                try
                {
                    geoNameIdParam.Value = city.GeoNameId;
                    benchmarkIdParam.Value = city.BenchmarkId;
                    parentIdParam.Value = parentId;
                    nameParam.Value = city.AsciiName;

                    var result = Connection.ExecuteNonQuery(command, false);
                    if (result < 0)
                        throw new Exception("Bad result from InsertGeographicLocation");
                }
                catch (Exception ex)
                {
                    Error.WriteLine($"Error inserting city {city.AsciiName}", ex);
                }
            }
        }
    }
  
    private void ImportAliases()
    {
        using var command = Connection.CreateCommand($"[{_geoNamesDatabase}].[dbo].[InsertGeographicLocationAlias]", false);
        command.CommandType = CommandType.StoredProcedure;

        var benchmarkIdParam = command.Parameters.Add("@BenchmarkId", SqlDbType.BigInt, 8);
        var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar, 512);
        command.Parameters.Add("@PracticeAreaId", SqlDbType.Int).Value = _practiceAreaId;
        command.Parameters.Add("@CreationDate", SqlDbType.DateTime2).Value = _creationDate;
        command.Parameters.Add("@CreatorId", SqlDbType.Int).Value = _creatorId;
        command.Parameters.Add("@CreationSession", SqlDbType.UniqueIdentifier).Value = _creationSession;

       var countryCodesToBenchmarkIds = Connection.Select(
            $"""
                SELECT [CountryCode], [BenchmarkId]
                FROM [{_geoNamesDatabase}].[dbo].[Entity]
                WHERE [FeatureCode] = N'COUNTRY'
             """, 
            (r) => new KeyValuePair<string, long>(r.GetString(0), r.GetInt64(1)),
            false);

        foreach (var country in countryCodesToBenchmarkIds)
        {
            var aliases = Connection.Select(
                $"""
                    SELECT E.[GeoNameId], A.[AlternateName]
                    FROM [{_geoNamesDatabase}].[dbo].[AlternateName] A
                    JOIN [{_geoNamesDatabase}].[dbo].[Entity] E ON (E.GeoNameId = A.GeoNameId)
                    WHERE E.[CountryCode] = N'{country.Key}'
                 """, 
                (r) => new { BenchmarkId = r.GetInt64(0), Name = r.GetString(1) },
                false);

            Log.WriteLine($"Importing {aliases.Count} aliases in country {country.Key} into Benchmark");

            foreach (var alias in aliases)
            {
                try
                {
                    benchmarkIdParam.Value = alias.BenchmarkId;
                    nameParam.Value = alias.Name;

                    var result = Connection.ExecuteNonQuery(command, false);
                    if (result < 0)
                        throw new Exception("Bad result from InsertGeographicLocationAlias");
                }
                catch (Exception ex)
                {
                    Error.WriteLine($"Error inserting alias {alias.BenchmarkId}:{alias.Name}", ex);
                }
            }
        }
    }

    private long AddGeographicLocationToDatabase(long parentId, string asciiName)
    {
        var name = asciiName.Replace("'", "''");
        var oid = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parentId} AND [Name] = N'{name}'",
            (r) => r.GetInt64(0), false);
        if (oid.HasValue)
            return oid.Value;

        var index = NextChildIndex(parentId);
        oid = Connection.GetNextOid();

        Connection.ExecuteNonQuery(
            $"""
                INSERT INTO [dbo].[t_GeographicLocation]
                    ([OID], [PID], [IsSystemOwned], [Name], [Index], [Description], [CreationDate], [CreatorId], [PracticeAreaID], [CreationSession])
                VALUES
                    ({oid.Value}, {parentId}, 1, N'{name}', {index}, N'{name}', '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, '{_creationSessionAsString}')
             """, false);   

        // We use the GeoNames AsciiName as the GL's name, and as the first (primary) alias
        AddAliasToDatabase(oid.Value, asciiName);
        return oid.Value;
    }

    private long NextChildIndex(long parentId)
    {
        long nextIndex;
        if (_previousChildIndex.ContainsKey(parentId))
        {
            nextIndex = _previousChildIndex[parentId] + 1;
        }
        else
        {
            var i = Connection.ExecuteScalar($"SELECT MAX([Index]) FROM [dbo].[t_GeographicLocation] WHERE [PID] = {parentId}", false);
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

    private void AddAliasToDatabase(long benchmarkId, string alias, string? description = null, long languageId = 500)
    {
        description ??= alias;

        var name = alias.Replace("'", "''");
        var lname = name.ToLower();
        var c = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE LOWER([Alias]) = '{lname}' AND [GeographicLocationId] = {benchmarkId}",
                                          (r) => r.GetInt64(0), 
                                          false);
        if (c.HasValue)
            return;

        var createAsPrimary = (languageId == 500);
        var c2 = Connection.SelectOneValue($"SELECT [OID] FROM [dbo].[t_GeographicLocationAlias] WHERE [IsPrimary] = 1 AND [GeographicLocationId] = {benchmarkId}", 
                                           (r) => r.GetInt64(0), 
                                           false);
        if (c2.HasValue)
            createAsPrimary = false;
        var p = createAsPrimary ? 1 : 0;

        var oid = Connection.GetNextOid();
        description = description.Replace("'", "''");
        Connection.ExecuteNonQuery("INSERT INTO [dbo].[t_GeographicLocationAlias] " +
            "([OID],[Alias],[Description],[IsSystemOwned],[IsPrimary],[CreationDate],[CreatorId],[PracticeAreaID],[GeographicLocationID],[LID],[CreationSession]) " +
            "VALUES " +
            $"({oid}, '{name}', '{description}', 1, {p}, '{_creationDateAsString}', {_creatorId}, {_practiceAreaId}, {benchmarkId}, {languageId},'{_creationSessionAsString}')", 
            false);
    }

    private void Dump()
    {
        using var file = File.CreateText("Dump.txt");
        DumpChildren(20000, 0, file);
    }

    private void DumpChildren(long benchmarkId, int indentation, StreamWriter writer)
    {
        var oids = Connection.Select($"SELECT [OID] FROM [dbo].[t_GeographicLocation] WHERE [PID] = {benchmarkId}", 
                                    (r) => r.GetInt64(0),
                                    false);
        foreach (var oid in oids)
        {
            var names = Connection.Select($"SELECT [Alias] FROM [dbo].[t_GeographicLocationAlias] WHERE [GeographicLocationID] = {oid} ORDER BY IsPrimary DESC, Alias", 
                                          (r) => r.GetString(0),
                                          false);
            
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
            DumpChildren(oid, indentation + 1, writer);
        }
   }
}
