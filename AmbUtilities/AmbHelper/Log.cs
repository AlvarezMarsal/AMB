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

    public static void Flush()
    {
        _logFile.Flush();
    }
}


public class LogFile : IDisposable
{
    private StreamWriter _logFile;

    public LogFile(string name)
    {
        var filename = name + ".txt";
        if (File.Exists(filename))
            File.Delete(filename);
        _logFile = File.CreateText(filename);

        AtExit.Add(() => { _logFile.Flush(); _logFile.Dispose(); });
    }

    public void WriteLine(string message)
    {
        Console.WriteLine(message);
        Debug.WriteLine(message);
        _logFile.WriteLine(message);
    }

    public void Dispose()
    {
        _logFile.Dispose();
    }

    public void Flush()
    {
        _logFile.Flush();
    }
}
