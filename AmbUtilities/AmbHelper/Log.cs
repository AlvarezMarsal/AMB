using System.Diagnostics;

namespace AmbHelper;

public static class Log
{
    public static readonly LogFile ApplicationLog;

    static Log()
    {
        var args = Environment.GetCommandLineArgs();
        var exe = args[0];
        var filename = Path.GetFileNameWithoutExtension(exe);
        ApplicationLog = new LogFile(filename);
    }

    public static bool Enabled { get => ApplicationLog.Enabled; set => ApplicationLog.Enabled = value; } 
    public static bool NoDebugger { get => ApplicationLog.NoDebugger; set => ApplicationLog.NoDebugger = value; }
    public static bool NoConsole { get => ApplicationLog.NoConsole; set => ApplicationLog.NoConsole = value; }

    public static LogFile WriteLine(string message)
    {
        return ApplicationLog.WriteLine(message);
    }

    public static LogFile WriteLine(Exception e)
    {
        return ApplicationLog.WriteLine(e.ToString());
    }
    public static LogFile WriteLine()
    {
        return ApplicationLog.WriteLine();
    }

    public static LogFile Flush()
    {
        return ApplicationLog.Flush();
    }

    public static LogFile Indent(int n=1) => ApplicationLog.Indent(n);

    public static LogFile Outdent(int n=1) => ApplicationLog.Outdent(n);
}


public class LogFile : IDisposable
{
    private StreamWriter _logFile;
    private int _indentLevel;
    private string _indentString = "";
    private static readonly char[] LineSeparators = ['\r', '\n'];
    private bool _disposed = false;
    public bool Enabled { get; set; } = true;
    public bool NoDebugger { get; set; }
    public bool NoConsole { get; set; }

    public LogFile(string name)
    {
        var filename = name + ".txt";
        if (File.Exists(filename))
            File.Delete(filename);
        _logFile = File.CreateText(filename);
        _logFile.WriteLine($"Log file created at {DateTime.Now}");

        AtExit.Add(() => { _logFile.Dispose(); });
    }

    public LogFile WriteLine(string message)
    {
        if (!Enabled)
            return this;
        if (message.IndexOfAny(LineSeparators) < 0)
            WriteIndentedLine(message);
        else
        {
            var lines = message.Split(LineSeparators);
            foreach (var line in lines)
            {
                if (line == null)
                    continue;
                var trimmed = line.Trim();
                if (trimmed.Length > 0)
                    WriteIndentedLine(trimmed);
            }
        }
        return this;
    }

    public LogFile WriteLine()
    {
        if (!Enabled)
            return this;
        if (!NoConsole)
            Console.WriteLine();
        //Debug.WriteLine("");
        _logFile.WriteLine();
        return this;
    }

    private LogFile WriteIndentedLine(string line)
    {
        if (_indentLevel > 0)
        {
            if (!NoConsole)
                Console.Write(_indentString);
            if (!NoDebugger)
                Debug.Write(_indentString);
            _logFile.Write(_indentString);
        }

        if (!NoConsole)
            Console.WriteLine(line);
        if (!NoDebugger)
            Debug.WriteLine(line);
        _logFile.WriteLine(line);
        return this;
    }


    public LogFile WriteLine(Exception ex) { if (Enabled) WriteLine(ex.ToString()); return this; }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _logFile.Flush();
            _logFile.Dispose();
        }
    }

    public LogFile Flush()
    {
        _logFile.Flush();
        return this;
    }

    public LogFile Indent(int n=1)  => SetIndentation(_indentLevel + n);

    public LogFile Outdent(int n=1) => SetIndentation(_indentLevel - n);

    private LogFile SetIndentation(int newIndentLevel)
    {
        if (newIndentLevel < 0)
            newIndentLevel = 0;
        else if (newIndentLevel > 20)
            newIndentLevel = 20;
        if (_indentLevel != newIndentLevel)
        {
            _indentLevel = newIndentLevel;
            _indentString = new string(' ', _indentLevel * 4);
        }
        return this;

    }
}
