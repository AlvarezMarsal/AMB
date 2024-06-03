using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        public bool IsOneBased => Package.Compatibility.IsWorksheets1Based;
        public readonly ColumnDefinitionCollection ColumnDefinitions;
        private bool _disposed;


        public ImportProcessor(Settings settings, Import import)
        {
            Settings = settings;
            Import = import;
            Package = new ExcelPackage(new FileInfo(Import.FilePath));
            Sheet = Package.Workbook.Worksheets[Import.Sheet];
            ColumnDefinitions = ColumnDefinition.PreprocessColumnDefinitions(Import, IsOneBased);
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
                        Console.WriteLine($"Detected end of data at row {row}");
                        return;
                    }
                    ++row;
                }
                catch (Exception e)
                {
                    var e1 = new Exception($"Error on row {row}", e);
                    Console.WriteLine(e1);
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
                var jt = ColumnDefinitions["JobTitleName"];
                Debug.Assert(jt.CurrentValue != "");
                if (jt.CurrentValue != "")
                    Console.WriteLine($"Processing row {row} with Alias = {alias.CurrentValue} and JobTitle = {jt.CurrentValue}");
            }

            // Process each cell in turn.  The way PreprocessColumnDefinitions works, we are guaranteed
            // that parents are processed before children.
            foreach (var cd in ColumnDefinitions)
            {
                if (cd.AssignedValue == null)
                    ProcessCell(cd);
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

        private void ProcessCell(ColumnDefinition cd)
        {
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
