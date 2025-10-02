namespace Shared;

using System.Diagnostics;
using System.Reflection;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;

public static class MonitorService
{
    public static readonly string ServiceName =
        Assembly.GetEntryAssembly()?.GetName().Name
        ?? Assembly.GetCallingAssembly().GetName().Name
        ?? "UnknownService";


    public static readonly ActivitySource ActivitySource = new(ServiceName);


    public static readonly TracerProvider TracerProvider;


    public static ILogger Log => Serilog.Log.Logger;


    static MonitorService()
    {
        var zipkinEndpoint = Environment.GetEnvironmentVariable("ZIPKIN__ENDPOINT")
                             ?? "http://localhost:9411/api/v2/spans";
        var seqUrl = Environment.GetEnvironmentVariable("SEQ__URL")
                     ?? "http://localhost:5341";


        TracerProvider = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(ServiceName))
            .AddSource(ActivitySource.Name)
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddZipkinExporter(o => o.Endpoint = new Uri(zipkinEndpoint))
            .Build();


        Serilog.Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Verbose()
            .Enrich.FromLogContext()
            .Enrich.WithProperty("service", ServiceName)
            .WriteTo.Console()
            .WriteTo.Seq(seqUrl)
            .CreateLogger();
    }
}