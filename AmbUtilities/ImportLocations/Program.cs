using OfficeOpenXml;
using OfficeOpenXml.FormulaParsing.Excel.Functions.Math;
using System.Configuration;

namespace ImportLocations
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("ImportLocations excelFileName sheetName rangeName server database");
                return;
            }

            try
            {
                var program = new Program();
                program.Run(args);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private void Run(string[] args)
        {
            int argIndex = 0;
            var excelFilename = args[argIndex++];
            if (!File.Exists(excelFilename))
                throw new FileNotFoundException(excelFilename);
            var sheetName = args[argIndex++];
            var rangeName = args[argIndex++];

            using var package = new ExcelPackage(new FileInfo(excelFilename));
            var zero = package.Compatibility.IsWorksheets1Based ? 1 : 0;
            using var sheet = package.Workbook.Worksheets[sheetName];

            if (!ParseAddress(rangeName, out var top, out var left, out var bottom, out var right))
                throw new ArgumentException("Invalid range");

            for (var row=top; row<=bottom; ++row)
            {
                var column = left;
                var continent = sheet.Cells[row, column++].Text.Trim();
                var country = sheet.Cells[row, column++].Text.Trim();
                var alias1 = sheet.Cells[row, column++].Text.Trim();
                var alias2 = sheet.Cells[row, column++].Text.Trim();
                var state = sheet.Cells[row, column++].Text.Trim();
                var city = sheet.Cells[row, column++].Text.Trim();
                var cityAscii = sheet.Cells[row, column++].Text.Trim();
            }

        }

        private static bool ParseAddress(string address, out int row, out int column)
        {
            var a = new ExcelAddressBase(address);
            row = a.Start.Row;
            column = a.Start.Column;
            return true;
        }

        private static bool ParseAddress(string address, out int top, out int left, out int bottom, out int right)
        {
            var a = new ExcelAddressBase(address);
            top = a.Start.Row;
            left = a.Start.Column;
            bottom = a.End.Row;
            right = a.End.Column;
            return true;
        }
    }
}
