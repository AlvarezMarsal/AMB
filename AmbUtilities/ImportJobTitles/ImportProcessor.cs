using System.Diagnostics;
using AmbHelper;
using OfficeOpenXml;
using static AmbHelper.Settings;

namespace ImportJobTitles
{
    internal class ImportProcessor : IDisposable
    {
        public readonly Settings Settings;
        public readonly Import Import;
        public readonly ExcelPackage Package;
        public readonly ExcelWorksheet Sheet;
        public readonly AmbDbConnection Connection;
        public bool IsOneBased => Package.Compatibility.IsWorksheets1Based;
        public readonly ColumnDefinitionCollection ColumnDefinitions;
        private bool _disposed;


        public ImportProcessor(Settings settings, Import import, AmbDbConnection connection)
        {
            Settings = settings;
            Import = import;
            Package = new ExcelPackage(new FileInfo(Import.FilePath));
            Sheet = Package.Workbook.Worksheets[Import.Sheet];
            ColumnDefinitions = ColumnDefinition.PreprocessColumnDefinitions(Import, IsOneBased);
            Connection = connection;
        }


        public void Run()
        {
            var row = Import.FirstRow;
            var end = (Import.LastRow < 0) ? Sheet.Dimension.Rows : Import.LastRow;

            var taxonomyColumn = ColumnDefinitions["Taxonomy"];
            var jobTitleNameColumn = ColumnDefinitions["JobTitleName"];
            var jobTitleDescriptionColumn = ColumnDefinitions["JobTitleDescription"];
            var aliasColumn = ColumnDefinitions["Alias"];
            var aliasDescriptionColumn = ColumnDefinitions["AliasDescription"];
            var taxonomyId = 0L;
            var jobTitleId = 0L;

            for (/**/; row <= end; ++row)
            {
                var rowIsEmpty = true;

                try
                {
                    Log.WriteLine($"Processing row {row}");
                    // Ensure we have a taxonomy ID to work with
                    var value = Sheet.Cells[row, taxonomyColumn.ColumnNumber].Text.Trim();
                    if (value != "")
                    {
                        var ptn = ParseTaxonomyName(value);
                        taxonomyId = GetTaxonomyNodeId(ptn);
                        jobTitleId = 0;
                        if (taxonomyId == 0)
                        {
                            taxonomyId = CreateNewTaxonomyNode(ptn);
                            if (taxonomyId == 0)
                            {
                                Log.WriteLine($"    Error: taxonomy node {ptn} does not exist and cannot be created");
                                break; // cannot continue without a valid taxonomy node
                            }
                            Log.WriteLine($"    Created taxonomy node {ptn}");
                        }
                        rowIsEmpty = false;
                    }

                    // Create the job title, if necessary
                    value = Sheet.Cells[row, jobTitleNameColumn.ColumnNumber].Text.Trim();
                    if (value != "")
                    {
                        if (taxonomyId == 0)
                        {
                            Log.WriteLine($"    Error: Missing taxonomy node information");
                            continue;
                        }

                        jobTitleId = GetJobTitleId(taxonomyId, value);
                        if (jobTitleId == 0)
                        {
                            jobTitleId = CreateNewJobTitle(taxonomyId, value, Sheet.Cells[row, jobTitleDescriptionColumn.ColumnNumber].Text.Trim());
                            if (jobTitleId == 0)
                            {
                                Log.WriteLine($"    Error: job title '{value}' does not exist and cannot be created");
                                break; // cannot continue without a valid job title
                            }
                            Log.WriteLine($"    Created job title '{value}'");
                        }
                        else
                        {
                            Log.WriteLine($"    Job title '{value}' already exists");
                        }
                        rowIsEmpty = false;
                    }

                    // If there's an alias, add it
                    value = Sheet.Cells[row, aliasColumn.ColumnNumber].Text.Trim();
                    if (value != "")
                    {
                        if (taxonomyId == 0)
                        {
                            Log.WriteLine($"    Error: Missing taxonomy node information");
                            continue;
                        }

                        if (jobTitleId == 0)
                        {
                            Log.WriteLine($"    Error: Missing job title information");
                            continue;
                        }
                        var aid = GetJobTitleAliasId(jobTitleId, value);
                        if (aid == 0)
                        {
                            aid = CreateNewJobTitleAlias(jobTitleId, value, Sheet.Cells[row, aliasDescriptionColumn.ColumnNumber].Text.Trim(), false);
                            if (aid == 0)
                            {
                                Log.WriteLine($"    Error: job title alias '{value}' does not exist and cannot be created");
                                continue;
                            }
                            Log.WriteLine($"    Created job title alias '{value}'");
                        }
                        else
                        {
                            Log.WriteLine($"    Job title alias '{value}' already exists");
                        }
                        rowIsEmpty = false;
                    }

                    if (rowIsEmpty)
                    {
                        //Log.WriteLine($"End of data found at row {row}");
                        //break;
                    }
                    Log.Flush();
                }
                catch (Exception e)
                {
                    var e1 = new Exception($"Error on row {row}", e);
                    Log.WriteLine("    Unhandled exception");
                    Log.WriteLine(e1.ToString());
                    if (Debugger.IsAttached)
                        Debugger.Break();
                    throw e1;
                }
            }
        }

        
        private long GetTaxonomyNodeId(string cellValue)
        {
            var ptn = ParseTaxonomyName(cellValue);
            return GetTaxonomyNodeId(ptn);
        }


        private long GetTaxonomyNodeId(ParsedTaxonomyName ptn)
        {
            var tnid = GetTaxonomyNodeIdByNumbers(ptn.Numbers, out var name);
            if (tnid > 0)
            {
                if (!name.EndsWith(ptn.Name))
                {
                    Log.WriteLine($"    Error: Taxonomy node {tnid} has different name: '{ptn.Name}' vs '{name}'");
                    return 0;
                }
                return tnid;
            }

            if (tnid == 0)
            {
                Log.WriteLine($"    Error: Taxonomy node not found: {ptn}");
                return 0;
            }
            else //if (tnid < 0)
            {
                Log.WriteLine($"    Error: Multiple taxonomy nodes found: {ptn}");
                return 0;
            }
        }


        private long GetTaxonomyNodeIdByNumbers(int[] numbers, out string name)
        {
            return GetTaxonomyNodeIdByNumbers(numbers, 0, numbers.Length, out name);
        }


        private long GetTaxonomyNodeIdByNumbers(int[] numbers, int offset, int count, out string name)
        {
            var oid = 100000L;
            name = "";
            for (var i=0; i<count; ++i) // there will be some numeric parts, and then the
            {
                var index = numbers[offset+i] - 1;
                var query = $"SELECT [OID], [Name] FROM [dbo].[vw_TaxonomyNode] WHERE PID = {oid} AND [Index] = {index}";
                var nodes = Connection.Select(query, r => new { Oid=r.GetInt64(0), Name=r.GetString(1)});
                if (nodes.Count == 0)
                    return 0;
                if (nodes.Count > 1)
                    return -1;
                oid = nodes[0].Oid;
                name = nodes[0].Name;
            }
            return oid;
        }

#if false
        private long GetTaxonomyNodeIdByNames(string[] names, out List<int> numbers)
        {
            numbers = new List<int>();
            var oid = 100000L;
            for (var i=0; i<names.Length; ++i) // there will be some numeric parts, and then the
            {
                var query = $"SELECT [OID], [Index] FROM [dbo].[vw_TaxonomyNode] WHERE PID = {oid} AND LOWER([Name]) = '{names[i]}'";
                var nodes = Connection.Select(query, r => new { Oid=r.GetInt64(0), Index=r.GetInt32(1)});
                if (nodes.Count == 0)
                    return 0;
                if (nodes.Count > 1)
                    return -1;
                oid = nodes[0].Oid;
                numbers.Add(nodes[0].Index);
            }
            return oid;
        }

#endif

        private record ParsedTaxonomyName(int[] Numbers, string Name)
        {
            public override string ToString()
            {
                var s = string.Join('.', Numbers) + " " + Name;
                return s;
            }
        }


        // '3.1.5.G&A - Finance - ERP - Finance and Accounting' --> ['3', '1', '5'] and 'G&A - Finance - ERP - Finance and Accounting'
        private ParsedTaxonomyName ParseTaxonomyName(string name)
        {
            // We do this in a way that makes it okay if the text part of the name has dots in it
            // (I don't think that happens, but it could)
            var lastDot = 0;
            for (var i=0; i<name.Length; ++i)
            {
                if (name[i] == '.')
                    lastDot = i;
                else if (!char.IsDigit(name[i]))
                    break;
            }

            var numericParts = name.Substring(0, lastDot).Split('.', StringSplitOptions.RemoveEmptyEntries);
            var numbers = new int[numericParts.Length];
            for (var i=0; i<numericParts.Length; ++i)
                numbers[i] = int.Parse(numericParts[i]);

            var textPart = name.Substring(lastDot+1).Trim();
            return new ParsedTaxonomyName(numbers, textPart);
        }


        private long CreateNewTaxonomyNode(ParsedTaxonomyName ptn)
        {
            // Find the parent node
            var pid = 100000L;
            byte type= 0;
            for (var i=0; i<ptn.Numbers.Length-1; ++i) // there will be some numeric parts, and then the text part
            {
                var index = ptn.Numbers[i] - 1;
                var query = $"SELECT [OID], [Type] FROM [dbo].[vw_TaxonomyNode] WHERE [PID] = {pid} AND [Index] = {index}";
                var nodes = Connection.Select(query, r => new { Oid=r.GetInt64(0), Type=r.GetByte(1)});
                if (nodes.Count == 0)
                    return 0;
                if (nodes.Count > 1)
                    return -1;
                pid = nodes[0].Oid;
                type = nodes[0].Type;
                //Log.WriteLine($"    Found parent node {pid} {type}");
            }

            // Insert the node
            var oid = Connection.GetNextOid();
            ++type;
            var query2 = $"""
                            INSERT INTO [dbo].[t_TaxonomyNode]
                            ([OID], [Version], [PracticeAreaID], [Type], [CreationSession], [CreationDate], [Creator])
                            VALUES
                            ({oid}, 0, 2501, {type}, '{Settings.CreationSession}', GETDATE(), {Settings.CreatorId})
                        """;
            var result = Connection.ExecuteNonQuery(query2);
            if (result != 1)
                return 0;

            string table = type switch
            {
                2 => "t_TaxonomyNodeFunctionGroup",
                3 => "t_TaxonomyNodeFunction",
                4 => "t_TaxonomyNodeProcessGroup",
                5 => "t_TaxonomyNodeProcess",
                6 => "t_TaxonomyNodeActivity",
                _ => throw new InvalidOperationException($"Invalid taxonomy node type {type}")
            };

            // Put it into the hierarchy
            var query3 = $"""
                            INSERT INTO [dbo].{table}
                            ([OID], [PID], [Index], [Name])
                            VALUES
                            ({oid}, {pid}, {ptn.Numbers[^1]-1}, '{ptn.Name}')
                        """;
            result = Connection.ExecuteNonQuery(query3);
            if (result != 1)
            {
                Connection.ExecuteNonQuery($"DELETE FROM [dbo].[t_TaxonomyNode] WHERE [OID] = {oid}");
                return 0;
            }

            return oid;
        }

        
        private long GetJobTitleId(long taxonomyId, string name)
        {
            name = name.Replace("'", "''").ToLower();
            var query = Settings.JobTitlesAreGloballyUnique 
                ? $"SELECT [OID] FROM [dbo].[t_JobTitle] WHERE LOWER([Name]) = '{name}'"
                : $"SELECT [OID] FROM [dbo].[t_JobTitle] WHERE TaxonomyId = {taxonomyId} AND LOWER([Name]) = '{name}'";
            var id = Connection.ExecuteScalar(query);   
            if (id is null)
                return 0; // indicating that it the job title doesn't exist
            if (id is not long nid)
                throw new InvalidOperationException($"    Error: Error retrieving job title {taxonomyId} {name}");
            return nid;
        }

        
        private long CreateNewJobTitle(long taxonomyId, string name, string description)
        {
            name = name.Replace("'", "''");

            if (string.IsNullOrEmpty(description))
                description = name;
            else
                description = description.Replace("'", "''");

            var oid = Connection.GetNextOid();

            var query = $"""
                            INSERT INTO [dbo].[t_JobTitle]
                            ([OID], [IsSystemOwned], [Name], [Description], [CreationDate], [CreatorID], [PracticeAreaID], [TaxonomyID], [CreationSession])
                            VALUES
                            ({oid}, 0, '{name}', '{description}', GETDATE(), {Settings.CreatorId}, 2501, {taxonomyId}, '{Settings.CreationSession}')
                        """;

            try
            {
                Connection.ExecuteNonQuery(query);
            }
            catch (Exception e)
            {
                Log.WriteLine($"    Error: error creating job title {taxonomyId} {name}");
                Log.WriteLine(e.ToString());
                return oid;
            }

            var aid = CreateNewJobTitleAlias(oid, name, description, true);
            return oid;
        }

        
        private long GetJobTitleAliasId(long jobTitleId, string name)
        {
            name = name.Replace("'", "''").ToLower();

            var query = Settings.JobTitleAliasesAreGloballyUnique 
                ? $"SELECT [OID] FROM [dbo].[t_JobTitleAlias] WHERE LOWER([Alias]) = '{name}'"
                : $"SELECT [OID] FROM [dbo].[t_JobTitleAlias] WHERE JobTitleID = {jobTitleId} AND LOWER([Alias]) = '{name}'";

            var id = Connection.ExecuteScalar(query);   
            if (id is null)
                return 0; // indicating that it the job title doesn't exist
            if (id is not long nid)
                throw new InvalidOperationException($"    Error: error retrieving job title alias {jobTitleId} {name}");
            return nid;
        }


        private long CreateNewJobTitleAlias(long jobTitleId, string name, string description, bool isPrimary)
        {
            var safeName = name.Replace("'", "''");
            if (string.IsNullOrEmpty(description))
                description = safeName;
            else
                description = description.Replace("'", "''");

            var oid = Connection.GetNextOid();
            var primary = isPrimary ? 1 : 0;

            var query = $"""
                            INSERT INTO [dbo].[t_JobTitleAlias]
                            ([OID], [Alias], [Description], [IsSystemOwned], [IsPrimary], [CreationDate], [CreatorID], [PracticeAreaID], [JobTitleID], [LID], [CreationSession])
                            VALUES
                            ({oid}, '{safeName}', '{description}', 0, {primary}, GETDATE(), {Settings.CreatorId}, 2501, {jobTitleId}, 500, '{Settings.CreationSession}')
                        """;

            try
            {
                Connection.ExecuteNonQuery(query);
            }
            catch (Exception e)
            {
                Log.WriteLine($"    Error: Error creating job title alias {jobTitleId} {name}");
                Log.WriteLine(e.ToString());
                return 0;
            }

            CreateNewJobTitleKeys(oid, name);

            return oid;
        }


        private void CreateNewJobTitleKeys(long aliasId, string alias)
        {
            alias = alias.Replace("'", "''").ToLower();
            var words = alias.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            var b = new System.Text.StringBuilder();
            b.Append($"INSERT INTO [dbo].[t_JobTitleAliasKeys] ([JobTitleAliasID],[Key]) VALUES ({aliasId}, '{words[0]}')");
            for (var i=1; i<words.Length; ++i)
                b.Append($", ({aliasId}, '{words[1]}')");

            try
            {
                Connection.ExecuteNonQuery(b.ToString());
            }
            catch (Exception e)
            {
                Log.WriteLine($"    Error: Error creating job title keys for {aliasId} {alias}");
                Log.WriteLine(e.ToString());
            }
        }

        #region Dispose

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    Sheet.Dispose();
                    Package.Dispose();
                }

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                _disposed = true;
            }
        }


        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
