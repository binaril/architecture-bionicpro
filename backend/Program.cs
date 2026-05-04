using Microsoft.AspNetCore.Authentication.JwtBearer;
using ReportsApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

builder.Services.AddScoped<IReportService, ReportService>();

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        // Authority = public-facing URL → sets the expected issuer to match token's "iss" claim.
        // MetadataAddress = internal Docker URL → used to fetch JWKS/discovery without leaving the cluster.
        // This split-horizon pattern is required when backend and browser use different hostnames for Keycloak.
        options.Authority = builder.Configuration["Keycloak:Authority"];
        options.MetadataAddress = builder.Configuration["Keycloak:MetadataAddress"];
        options.RequireHttpsMetadata = false;

        options.TokenValidationParameters = new()
        {
            // Audience validation skipped: the frontend client (reports-frontend) issues tokens
            // whose audience is "account", not "reports-api". Security is enforced by:
            //   1. Valid signature from this realm's JWKS
            //   2. User-self restriction in the controller (email claim match)
            ValidateAudience = false,
            // MetadataAddress points to the internal Docker hostname, so its discovery doc returns
            // "issuer: http://keycloak:8080/..." which doesn't match the token's
            // "iss: http://localhost:8080/...". Set ValidIssuer explicitly to the public-facing URL.
            ValidIssuer = builder.Configuration["Keycloak:Authority"]
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
