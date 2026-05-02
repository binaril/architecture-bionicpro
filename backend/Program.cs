using Microsoft.AspNetCore.Authentication.JwtBearer;
using ReportsApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddScoped<IReportService, ReportService>();

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        // Keycloak realm endpoint — used to fetch JWKS and validate token signature
        options.Authority = builder.Configuration["Keycloak:Authority"];
        options.RequireHttpsMetadata = false;

        options.TokenValidationParameters = new()
        {
            // Audience validation skipped: the frontend client (reports-frontend) issues tokens
            // whose audience is "account", not "reports-api". Security is enforced by:
            //   1. Valid signature from this realm's JWKS
            //   2. User-self restriction in the controller (email claim match)
            ValidateAudience = false
        };
    });

builder.Services.AddAuthorization();

builder.Services.AddCors(options =>
    options.AddDefaultPolicy(policy =>
        policy.WithOrigins("http://localhost:3000")
              .AllowAnyHeader()
              .AllowAnyMethod()));

var app = builder.Build();

app.UseCors();
app.UseAuthentication();
app.UseAuthorization();
app.MapControllers();

app.Run();
