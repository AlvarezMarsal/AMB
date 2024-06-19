using System.Diagnostics;

namespace AmbHelper;

public static class Log
{
    public static readonly LogFile ApplicationLog;
    public static readonly LogFile ErrorLog;
    public static bool Debug { get; set; } = true;
    public static bool Console { get; set; } = true;
    private static readonly char[] LineSeparators = ['\r', '\n'];

    static Log()
    {
        var args = Environment.GetCommandLineArgs();
        var exe = args[0];
        var filename = Path.GetFileNameWithoutExtension(exe);
        ApplicationLog = new LogFile(filename);
        ErrorLog = new LogFile(filename + ".Errors");
    }

    public static bool Enabled { get => ApplicationLog.Enabled; set => ApplicationLog.Enabled = value; } 

    public static LogFile WriteLine(string message)
    {
        Output(message, false);
        return ApplicationLog;
    }

    public static LogFile WriteLine(Exception e)
    {
        Output(e.ToString(), false);
        return ApplicationLog;
    }

    public static LogFile WriteLine()
    {
        Output("", false);
        return ApplicationLog;
    }

    public static LogFile Flush()
    {
        ErrorLog.Flush();
        ApplicationLog.Flush();
        return ApplicationLog;
    }

    public static LogFile Indent(int n=1) => ApplicationLog.Indent(n);

    public static LogFile Outdent(int n=1) => ApplicationLog.Outdent(n);

    public static LogFile Error(string message)
    {
        Output(message, true);
        return ApplicationLog;
    }

    public static LogFile Error(Exception e)
    {
        var s = e.ToString();
        Output(s, true);
        return ApplicationLog;
    }
   
    private static void Output(string text, bool error)
    {
        if (text.IndexOfAny(LineSeparators) < 0)
        {
            Really(text);
            return;
        }

        var lines = text.Split(LineSeparators, StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (trimmed.Length > 0)
                Really(trimmed);
        }

        return;

        void Really(string txt)
        {
            if (error)
            {
                ErrorLog.WriteLine(txt);
                ErrorLog.Flush();
            }

            if (Console)
                System.Console.WriteLine(txt);

            if (error || Debug)
                System.Diagnostics.Debug.WriteLine(txt);

            if (ApplicationLog.Enabled)
                ApplicationLog.WriteLine(txt);
        }
    }


    public static void Dispose()
    {
        ApplicationLog.Dispose();
        ErrorLog.Dispose();
    }

}


public class LogFile : IDisposable
{
    private StreamWriter _logFile;
    private int _indentLevel;
    private string _indentString = "";
    private bool _disposed = false;
    public bool Enabled { get; set; } = true;
    private DateTime _openTime;

    public LogFile(string name)
    {
        var filename = name + ".txt";
        if (File.Exists(filename))
            File.Delete(filename);
        _logFile = File.CreateText(filename);
        _openTime = DateTime.Now;
        _logFile.WriteLine($"Log file opened at {_openTime}");

        AtExit.Add(() => { _logFile.Dispose(); });
    }

    public LogFile WriteLine(string message)
    {
        if (Enabled)
        {
            _logFile.Write(_indentString);
            _logFile.WriteLine(message);
        }
        return this;
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            var closeTime = DateTime.Now;
            _logFile.WriteLine($"Log file closed at {closeTime}");
            var d = closeTime - _openTime;
            _logFile.WriteLine($"Duration {d}");
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



