using System.Diagnostics;
using System.Security.Cryptography;
using System.Xml.Linq;
using AmbHelper;
using OfficeOpenXml;
using static System.Runtime.InteropServices.JavaScript.JSType;
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
            var row = (Import.FirstRow < 0) ? FindFirstRow() : Import.FirstRow;
            var end = (Import.LastRow < 0) ? Sheet.Dimension.Rows : Import.LastRow;

            while (row <= end)
            {
                try
                {
                    LoadRowValues(row);
                    var ok = ProcessRow(row);
                    if (!ok)
                    {
                        Log.WriteLine($"Detected end of data at row {row}");
                        return;
                    }
                    ++row;
                }
                catch (Exception e)
                {
                    var e1 = new Exception($"Error on row {row}", e);
                    Log.WriteLine(e1.ToString());
                    if (Debugger.IsAttached)
                        Debugger.Break();
                    throw e1;
                }
            }
        }


        private int FindFirstRow()
        {
            var first = IsOneBased ? 1 : 0;
            for (var i=first; i<Sheet.Dimension.Rows; i++)
            {
                if (LoadRowValues(i))
                    return i;
            }
            throw new Exception("No data found in the spreadsheet");
        }


        private bool LoadRowValues(int row)
        {
            var isEmpty = true;
            foreach (var cd in ColumnDefinitions)
            {
                var cv = Sheet.Cells[row, cd.ColumnNumber].Text.Trim();
                if (string.IsNullOrEmpty(cv))
                {
                    if (cd.BlankIsDitto)
                    {
                        if (cd.CurrentValue != "")
                            isEmpty = false;
                    }
                    else
                    {
                        cd.CurrentValue = "";
                        cd.AssignedValue = null;
                    }
                }
                else
                {
                    isEmpty = false;
                    if (cd.CurrentValue != cv)
                    {
                        cd.CurrentValue = cv;
                        cd.AssignedValue = null;
                    }
                }
            }
            return !isEmpty;
        }


        private bool ProcessRow(int row)
        {
            if (row % 100 == 0)
                Console.WriteLine($"Processing row {row}");

            // See if we should ignore the row, or if we have reached the end of the data
            foreach (var cd in ColumnDefinitions)
            {
                // There are values that cause the row to be excluded
                // if (cd.Exclusions.Contains(cd.CurrentValue))
                //    return true;

                //if (Debugger.IsAttached && Breakpoints.Contains(cd.CurrentValue))
                //{
                //    Debug.WriteLine(cd.ToString());
                //    Debugger.Break();
                //}

                // If a non-optional cell is empty, that indicates the end of the data
                // (if we don't have a predefined range).
                if (string.IsNullOrEmpty(cd.CurrentValue) && !cd.SettingsDefinition.Optional)
                {
                    if (Import.LastRow < 0)
                    {
                        Console.WriteLine($"Detected end of data");
                        return false;
                    }
                    throw new InvalidOperationException($"No value for cell {cd.ColumnNumber}{row}");
                }
            }

            var alias = ColumnDefinitions["Alias"];
            if (alias.CurrentValue != "")
            {
                // Check the taxonomy
                var tColumn = ColumnDefinitions["Taxonomy"];
                if (tColumn.AssignedValue is not long tnid)
                {
                    tnid = GetTaxonomyNodeId(tColumn.CurrentValue);
                    tColumn.AssignedValue = tnid;
                }

                // See if the job title is defined
                var jtColumn = ColumnDefinitions["JobTitleName"];
                if (jtColumn.AssignedValue is not long jtid)
                {
                    jtid = GetJobTitleId(tnid, jtColumn.CurrentValue);
                    if (jtid == 0)
                    {
                        jtid = CreateNewJobTitle(tnid, jtColumn.CurrentValue, ColumnDefinitions["JobTitleDescription"].CurrentValue);
                        if (jtid == 0)
                        {
                            Log.WriteLine($"Attempted to create new job title {tnid} {jtColumn.CurrentValue} but failed");
                            return true;
                        }
                        ColumnDefinitions["JobTitleName"].AssignedValue = jtid;
                        if (alias.CurrentValue == jtColumn.CurrentValue)
                            return true; // the primary alias is already created
                    }
                    else
                    {
                        ColumnDefinitions["JobTitleName"].AssignedValue = jtid;
                    }
                }

                var aid = GetJobTitleAliasId(jtid, alias.CurrentValue);
                if (aid == 0)
                {
                    aid = CreateNewJobTitleAlias(jtid, alias.CurrentValue, ColumnDefinitions["AliasDescription"].CurrentValue, false);
                    if (jtid == 0)
                    {
                        Log.WriteLine($"Attempted to create new job title alias {alias.CurrentValue} for {jtColumn.CurrentValue} but failed");
                        return true;
                    }
                }
                else
                {
                    Log.WriteLine($"Job Title Alias {alias.CurrentValue} already exists for {jtColumn.CurrentValue}");
                }
            }

 

            /*
            // Now for aliases
            foreach (var cd in columnDefinitions)
            {
                if (!string.IsNullOrEmpty(cd.CurrentValue) && (cd.AliasOf != null))
                {
                    AddAliasIfNotPresent(cd.AliasOf.AssignedGeographicLocation!, cd.CurrentValue, false);
                }
            }
            */

            return true;
        }

        private long GetTaxonomyNodeId(string cellValue)
        {
            string[] tables = { "t_TaxonomyNodeFunctionGroup", "t_TaxonomyNodeFunction", "t_TaxonomyNodeProcessGroup", "t_TaxonomyNodeProcess" };

            var parts = cellValue.Split('.'); // '3.1.5.G&A - Finance - ERP - Finance and Accounting' --> ['3', '1', '5', 'G&A - Finance - ERP - Finance and Accounting']
            var oid = 100000L;
            for (var i=0; i<parts.Length; ++i)
            {
                if (!int.TryParse(parts[i]!, out var index))
                    break;

                var query = $"SELECT [OID] FROM [dbo].[{tables[i]}] WHERE PID = {oid} AND [Index] = {index-1}";
                var o = Connection.ExecuteScalar(query);
                if (o is not long pid)
                    throw new InvalidOperationException($"Unknown {tables[i]} index {index-1}");
                oid = pid;
            }
            return oid;
        }

        private long GetJobTitleId(long taxonomyId, string name)
        {
            var query = $"SELECT [OID] FROM [dbo].[t_JobTitle] WHERE TaxonomyId = {taxonomyId} AND Name = '{name}'";
            var id = Connection.ExecuteScalar(query);   
            if (id is null)
                return 0; // indicating that it the job title doesn't exist
            if (id is not long nid)
                throw new InvalidOperationException($"Error retrieving job title {taxonomyId} {name}");
            return nid;
        }

        private long CreateNewJobTitle(long taxonomyId, string name, string description)
        {
            if (string.IsNullOrEmpty(description))
                description = name;

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
                Log.WriteLine($"Error creating job title {taxonomyId} {name}");
                Log.WriteLine(e.ToString());
                return oid;
            }

            var aid = CreateNewJobTitleAlias(oid, name, description, true);
            return oid;
        }

        private long GetJobTitleAliasId(long jobTitleId, string name)
        {
            var query = $"SELECT [OID] FROM [dbo].[t_JobTitleAlias] WHERE JobTitleID = {jobTitleId} AND Alias = '{name}'";
            var id = Connection.ExecuteScalar(query);   
            if (id is null)
                return 0; // indicating that it the job title doesn't exist
            if (id is not long nid)
                throw new InvalidOperationException($"Error retrieving job title alias {jobTitleId} {name}");
            return nid;
        }


       private long CreateNewJobTitleAlias(long jobTitleId, string name, string description, bool isPrimary)
        {
            if (string.IsNullOrEmpty(description))
                description = name;

            var oid = Connection.GetNextOid();
            var primary = isPrimary ? 1 : 0;

            var query = $"""
                            INSERT INTO [dbo].[t_JobTitleAlias]
                            ([OID], [Alias], [Description], [IsSystemOwned], [IsPrimary], [CreationDate], [CreatorID], [PracticeAreaID], [JobTitleID], [LID], [CreationSession])
                            VALUES
                            ({oid}, 0, '{name}', '{description}', 0, {primary}, GETDATE(), {Settings.CreatorId}, 2501, {jobTitleId}, 500, '{Settings.CreationSession}')
                        """;

            try
            {
                Connection.ExecuteNonQuery(query);
            }
            catch (Exception e)
            {
                Log.WriteLine($"Error creating job title alias {jobTitleId} {name}");
                Log.WriteLine(e.ToString());
                return 0;
            }

            return oid;
       }

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
    }
}
