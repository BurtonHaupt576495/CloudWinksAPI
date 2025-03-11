using CloudWinksServiceAPI.Data;
using CloudWinksServiceAPI.Interfaces;
using CloudWinksServiceAPI.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);


// Add database context for FrameworkDbContext
builder.Services.AddDbContext<SpinacheDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

// Initialize DatabaseConnectionManager with default connection string
string defaultConnectionString = builder.Configuration.GetConnectionString("DefaultConnection");
if (string.IsNullOrEmpty(defaultConnectionString))
{
    throw new InvalidOperationException("DefaultConnection is not configured in appsettings.json.");
}
builder.Services.AddSingleton(new DatabaseConnectionManager(defaultConnectionString));

// Add custom services
builder.Services.AddScoped<IDynamicQueryService, DynamicQueryService>();



// Add CORS services
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowFlutterApp", policy =>
    {
        policy.WithOrigins("*") // Replace with your Flutter app's URL
              .AllowAnyHeader()                     // Allow all headers
              .AllowAnyMethod();                    // Allow all HTTP methods
    });
});

// Add controllers
builder.Services.AddControllers();

// Add Swagger services for API documentation
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo
    {
        Title = "CloudWinks Service API",
        Version = "v1",
        Description = "API documentation for your project."
    });
});

// Ensure IConfiguration is available for dependency injection
var configuration = builder.Configuration;

// Build the app
var app = builder.Build();

// Serve static files (Swagger UI files are static)
app.UseStaticFiles();

// Enable Swagger UI and Swagger JSON endpoint
if (app.Environment.IsDevelopment() || app.Environment.IsProduction())
{
    app.UseDeveloperExceptionPage(); // Enables detailed error messages
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "CloudWinks Service API V1");
        c.DefaultModelsExpandDepth(-1); // Optional: Removes "Schemas" section in Swagger UI
    });
}
else
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "CloudWinks Service API V1");
        c.RoutePrefix = ""; // Swagger UI at /swagger
    });
}

// Enable CORS globally
app.UseCors("AllowFlutterApp");

// Enable routing
app.UseRouting();

// Use HTTPS redirection
app.UseHttpsRedirection();

// Enable authorization
app.UseAuthorization();

// Map controllers
app.MapControllers();

// Run the app
app.Run();
