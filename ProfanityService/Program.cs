using Microsoft.EntityFrameworkCore;
using ProfanityService.Data;
using Prometheus;

var builder = WebApplication.CreateBuilder(args);

// DB CONTEXT (LIKE YOUR OTHER SERVICES)
builder.Services.AddDbContext<ProfanityDbContext>(opt =>
    opt.UseNpgsql(Environment.GetEnvironmentVariable("PROFANITY_DB") ?? "Host=profanity-db;Database=profanity;Username=postgres;Password=postgres"));

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// MIDDLEWARE
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpMetrics();
app.MapMetrics();
app.MapControllers();
app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

// AUTO-MIGRATE + SEED (YOUR STYLE — RAW SQL)
var ddl = """
          CREATE TABLE IF NOT EXISTS "ProfaneWords" (
              "Id" SERIAL PRIMARY KEY,
              "Word" TEXT NOT NULL
          );
          CREATE UNIQUE INDEX IF NOT EXISTS "IX_ProfaneWords_Word" ON "ProfaneWords" ("Word");
          """;

var seed = """
           INSERT INTO "ProfaneWords" ("Word") VALUES 
           ('damn'), ('hell'), ('crap'), ('f*ck'), ('sh*t')
           ON CONFLICT ("Word") DO NOTHING;
           """;

using var db = app.Services.GetRequiredService<ProfanityDbContext>();
db.Database.ExecuteSqlRaw(ddl);
db.Database.ExecuteSqlRaw(seed);

app.Run();