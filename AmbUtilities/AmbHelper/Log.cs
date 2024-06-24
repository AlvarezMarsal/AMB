namespace AmbHelper;

public static class Logs
{
    public static readonly ILogWriter Log;
    public static readonly ILogWriter Error;

    private static readonly char[] LineSeparators = ['\r', '\n'];

    static Logs()
    {
        var args = Environment.GetCommandLineArgs();
        var exe = args[0];
        var filename = Path.GetFileNameWithoutExtension(exe);
        Log = new LogFile(filename);
        Error = new ErrorLogFile(filename + ".Errors", Log);
    }

    public static void Dispose()
    {
        Log.Dispose();
        Error.Dispose();
    }
}

public interface ILogWriter : IDisposable
{
    bool Debug { get; set; }
    bool Console { get; set; }
    bool Enabled { get; set; }

    ILogWriter WriteLine();
    ILogWriter WriteLine(string text);
    ILogWriter WriteLine(Exception ex);
    ILogWriter WriteLine(string text, Exception ex);

    ILogWriter Indent(int n=1);
    ILogWriter Outdent(int n=1);
    ILogWriter Flush();

}

public abstract class BaseLogWriter : ILogWriter
{
    private int _indentLevel;
    protected string IndentString = "";
    protected bool Disposed = false;
    private static readonly char[] LineSeperators = { '\r', '\n' };
    public bool Enabled { get; set; } = true;
    public bool Debug { get; set; } = true;
    public bool Console { get; set; } = true;

    public BaseLogWriter()
    {
        AtExit.Add(() => { Dispose(); });
    }

    public ILogWriter WriteLine() => WriteLine("");
    public ILogWriter WriteLine(Exception ex) => WriteLine(ex.ToString());
    public ILogWriter WriteLine(string message, Exception ex) { WriteLine(message); return WriteLine(ex); }

    public ILogWriter WriteLine(string message)
    {
        if (Enabled)
        {
            if (message.IndexOfAny(LineSeperators) >= 0)
            {
                OutputLine(message);
            }
            else
            {
                var lines = message.Split(LineSeperators, StringSplitOptions.RemoveEmptyEntries);
                foreach (var line in lines)
                {
                    OutputLine(line);
                }
            }
        }

        return this;
    }

    protected virtual void OutputLine(string line)
    {
        if (Console)
        {
            if (line.Length > 0)
                System.Console.Write(IndentString);
            System.Console.WriteLine(line);
        }

        if (Debug)
        {
            if (line.Length > 0)
                System.Diagnostics.Debug.Write(IndentString);
            System.Diagnostics.Debug.WriteLine(line);
        }
    }

    public virtual void Dispose()
    {
        if (!Disposed)
        {
            Disposed = true;
            Flush();
        }
    }

    public virtual ILogWriter Flush()
    {
        return this;
    }

    public ILogWriter Indent(int n=1)  => SetIndentation(_indentLevel + n);

    public ILogWriter Outdent(int n=1) => SetIndentation(_indentLevel - n);

    private ILogWriter SetIndentation(int newIndentLevel)
    {
        if (newIndentLevel < 0)
            newIndentLevel = 0;
        else if (newIndentLevel > 20)
            newIndentLevel = 20;
        if (_indentLevel != newIndentLevel)
        {
            _indentLevel = newIndentLevel;
            IndentString = new string(' ', _indentLevel * 4);
        }
        return this;
    }
}


public class LogFile : BaseLogWriter
{
    protected StreamWriter Writer;
    private DateTime _openTime;

    public LogFile(string name)
    {
        var filename = name + ".txt";
        if (File.Exists(filename))
            File.Delete(filename);
        Writer = File.CreateText(filename);
        _openTime = DateTime.Now;
        Writer.WriteLine($"Log file opened at {_openTime}");
    }

    public override void Dispose()
    {
        if (!Disposed)
        {
            var closeTime = DateTime.Now;
            WriteLine($"Log file closed at {closeTime}");
            var d = closeTime - _openTime;
            WriteLine($"Duration {d}");
            base.Dispose();
        }
    }

    public override ILogWriter Flush()
    {
        Writer.Flush();
        return this;
    }

    protected override void OutputLine(string line)
    {
        if (line.Length > 0)
            Writer.Write(IndentString);
        Writer.WriteLine(line);
        base.OutputLine(line);
    }
}


public class ErrorLogFile : LogFile, IDisposable
{
    private ILogWriter _applicationLogWriter;

    public ErrorLogFile(string name, ILogWriter appLog) : base(name)
    {
        _applicationLogWriter = appLog;
    }

    protected override void OutputLine(string line)
    {
        Writer.Write(IndentString);
        Writer.WriteLine(line);

        if (Console && !(_applicationLogWriter.Enabled && _applicationLogWriter.Console))
        {
            if (line.Length > 0)
                System.Console.Write(IndentString);
            System.Console.WriteLine(line);
        }

        if (Debug && !(_applicationLogWriter.Enabled && _applicationLogWriter.Debug))
        {
            if (line.Length > 0)
                System.Diagnostics.Debug.Write(IndentString);
            System.Diagnostics.Debug.WriteLine(line);
        }
    }

    public override void Dispose()
    {
        _applicationLogWriter.Dispose();
        base.Dispose();
    }

    public override ILogWriter Flush()
    {
        _applicationLogWriter.Flush();
        base.Flush();
        return this;
    }
}
