using Npgsql;
using ReportsApi.Models;
using System.Text.Json;

namespace ReportsApi.Services;

public interface IReportService
{
    Task<CustomerReport?> GetReportByEmailAsync(string email);
}

public class ReportService : IReportService
{
    private readonly string _connectionString;

    public ReportService(IConfiguration configuration)
    {
        _connectionString = configuration.GetConnectionString("OlapDb")
            ?? throw new InvalidOperationException("OlapDb connection string is not configured");
    }

    public async Task<CustomerReport?> GetReportByEmailAsync(string email)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();

        const string sql = """
            SELECT customer_id, customer_name, email, phone, company, crm_created_at,
                   device_count, active_devices, device_types,
                   total_events, avg_event_value, max_event_value, min_event_value,
                   first_event_at, last_event_at, events_30d, avg_value_30d,
                   events_by_type, mart_updated_at
            FROM mart_customer_telemetry
            WHERE email = @email
            """;

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("email", email);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new CustomerReport
        {
            CustomerId    = reader.GetInt32(0),
            CustomerName  = reader.IsDBNull(1)  ? null : reader.GetString(1),
            Email         = reader.IsDBNull(2)  ? null : reader.GetString(2),
            Phone         = reader.IsDBNull(3)  ? null : reader.GetString(3),
            Company       = reader.IsDBNull(4)  ? null : reader.GetString(4),
            CrmCreatedAt  = reader.IsDBNull(5)  ? null : reader.GetDateTime(5),
            DeviceCount   = reader.IsDBNull(6)  ? 0    : reader.GetInt32(6),
            ActiveDevices = reader.IsDBNull(7)  ? 0    : reader.GetInt32(7),
            DeviceTypes   = reader.IsDBNull(8)  ? null : reader.GetString(8),
            TotalEvents   = reader.IsDBNull(9)  ? 0    : reader.GetInt64(9),
            AvgEventValue = reader.IsDBNull(10) ? null : reader.GetDecimal(10),
            MaxEventValue = reader.IsDBNull(11) ? null : reader.GetDecimal(11),
            MinEventValue = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
            FirstEventAt  = reader.IsDBNull(13) ? null : reader.GetDateTime(13),
            LastEventAt   = reader.IsDBNull(14) ? null : reader.GetDateTime(14),
            Events30d     = reader.IsDBNull(15) ? 0    : reader.GetInt64(15),
            AvgValue30d   = reader.IsDBNull(16) ? null : reader.GetDecimal(16),
            EventsByType  = reader.IsDBNull(17) ? null
                : JsonSerializer.Deserialize<Dictionary<string, object>>(reader.GetString(17)),
            MartUpdatedAt = reader.GetDateTime(18)
        };
    }
}
