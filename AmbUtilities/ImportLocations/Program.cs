using OfficeOpenXml;
using System.Configuration;

namespace ImportLocations
{
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 3)
            {
                Console.WriteLine("ImportLocations excelFileName server database");
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
            var excelFilename = args[0];
            if (!File.Exists(excelFilename))
                throw new FileNotFoundException(excelFilename);
            using var package = new ExcelPackage(new FileInfo(excelFilename));
            Console.ReadKey();
        }
    }
}
