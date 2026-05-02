using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using ReportsApi.Services;

namespace ReportsApi.Controllers;

[ApiController]
[Route("[controller]")]
[Authorize]
public class ReportsController : ControllerBase
{
    private readonly IReportService _reportService;

    public ReportsController(IReportService reportService)
    {
        _reportService = reportService;
    }

    /// <summary>
    /// Returns the pre-aggregated OLAP report for the authenticated user.
    /// Access is restricted to the caller's own data: the email claim from the
    /// JWT is used as the lookup key, so a user cannot request another user's report.
    /// </summary>
    [HttpGet]
    public async Task<IActionResult> GetMyReport()
    {
        // Extract email from Keycloak JWT (requires "email" scope on the client)
        var email = User.FindFirst("email")?.Value
                 ?? User.FindFirst(System.Security.Claims.ClaimTypes.Email)?.Value;

        if (string.IsNullOrEmpty(email))
            return Unauthorized(new { error = "Token does not contain an email claim." });

        var report = await _reportService.GetReportByEmailAsync(email);

        if (report is null)
            return NotFound(new { error = $"No report found for {email}." });

        return Ok(report);
    }
}
