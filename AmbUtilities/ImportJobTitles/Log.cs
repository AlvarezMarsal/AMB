using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ImportJobTitles
{
    internal static class Log
    {
        static StreamWriter _logFile;

        static Log()
        {
            _logFile = File.CreateText("ImportJobTitles.log");
        }

        public static void WriteLine(string message)
        {
            Console.WriteLine(message);
            Debug.WriteLine(message);
            _logFile.WriteLine(message);
        }
    }
}
