using System.Diagnostics.Metrics;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ProfanityService.Data;
using ProfanityService.Models;
using Prometheus;
using Shared;

namespace ProfanityService.Controllers;

[ApiController]
[Route("api/profanity")]
public class ProfanityController : ControllerBase
{
    private readonly ProfanityDbContext _db;

   
    private static readonly Histogram Duration = Metrics.CreateHistogram(
        "profanity_check_duration_seconds", "Profanity check latency");

    public ProfanityController(ProfanityDbContext db) => _db = db;

    [HttpPost("check")]
    public async Task<ActionResult<ProfanityResult>> Check([FromBody] ProfanityRequest req)
    {
        using var timer = Duration.NewTimer();

        var badWords = await _db.ProfaneWords
            .Select(w => w.Word)
            .ToListAsync();

        var found = badWords
            .Where(w => req.Text.Contains(w, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return Ok(new ProfanityResult(
            IsProfane: found.Any(),
            Matches: found
        ));
    }
}