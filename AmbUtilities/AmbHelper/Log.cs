using System.Diagnostics;

namespace AmbHelper;

public static class Log
{
    static LogFile _logFile;

    static Log()
    {
        var args = Environment.GetCommandLineArgs();
        var exe = args[0];
        var filename = Path.GetFileNameWithoutExtension(exe);
        _logFile = new LogFile(filename);
    }

    public static void WriteLine(string message)
    {
        _logFile.WriteLine(message);
    }

    public static void WriteLine(Exception e)
    {
        _logFile.WriteLine(e.ToString());
    }


    public static void Flush()
    {
        _logFile.Flush();
    }

    public static int Indent(int n=1) => _logFile.Indent(n);

    public static int Outdent(int n=1) => _logFile.Outdent(n);
}


public class LogFile : IDisposable
{
    private StreamWriter _logFile;
    private int _indentLevel;
    private string _indentString = "";
    private static readonly char[] LineSeparators = ['\r', '\n'];

    public LogFile(string name)
    {
        var filename = name + ".txt";
        if (File.Exists(filename))
            File.Delete(filename);
        _logFile = File.CreateText(filename);
        _logFile.WriteLine($"Log file created at {DateTime.Now}");

        AtExit.Add(() => { _logFile.Flush(); _logFile.Dispose(); });
    }

    public void WriteLine(string message)
    {
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
    }

    private void WriteIndentedLine(string line)
    {
        if (_indentLevel > 0)
        {
            Console.Write(_indentString);
            Debug.Write(_indentString);
            _logFile.Write(_indentString);
        }

        Console.WriteLine(line);
        Debug.WriteLine(line);
        _logFile.WriteLine(line);
    }


    public void WriteLine(Exception ex) => WriteLine(ex.ToString());

    public void Dispose()
    {
        _logFile.Dispose();
    }

    public void Flush()
    {
        _logFile.Flush();
    }

    public int Indent(int n=1)  => SetIndentation(_indentLevel + n);

    public int Outdent(int n=1) => SetIndentation(_indentLevel - n);

    private int SetIndentation(int n)
    {
        if (n < 0)
            n = 0;
        _indentLevel = n;
        _indentString = new string(' ', _indentLevel * 4);
        return n;

    }
}
