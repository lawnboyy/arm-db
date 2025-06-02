using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;

public static class XUnitLoggerExtensions
{
  public static ILoggingBuilder AddXUnit(this ILoggingBuilder builder, ITestOutputHelper output)
  {
    builder.Services.AddSingleton<ILoggerProvider>(new XUnitLoggerProvider(output));
    return builder;
  }
}

public class XUnitLoggerProvider : ILoggerProvider
{
  private readonly ITestOutputHelper _output;
  public XUnitLoggerProvider(ITestOutputHelper output) => _output = output;
  public ILogger CreateLogger(string categoryName) => new XUnitLogger(_output, categoryName);
  public void Dispose() { }
}

public class XUnitLogger : ILogger
{
  private readonly ITestOutputHelper _output;
  private readonly string _categoryName;
  public XUnitLogger(ITestOutputHelper output, string categoryName)
  {
    _output = output;
    _categoryName = categoryName;
  }
  public IDisposable BeginScope<TState>(TState state) => NoopDisposable.Instance;
  public bool IsEnabled(LogLevel logLevel) => true; // Log all levels passed by filter
  public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
  {
    try
    {
      _output.WriteLine($"{DateTime.Now:HH:mm:ss.fff} [{logLevel.ToString().Substring(0, 3).ToUpper()}] {_categoryName}{System.Environment.NewLine}      {formatter(state, exception)}");
      if (exception != null)
      {
        _output.WriteLine(exception.ToString());
      }
    }
    catch (Exception) { /* Can happen if test output is already disposed */ }
  }
  private class NoopDisposable : IDisposable { public static readonly NoopDisposable Instance = new(); public void Dispose() { } }
}