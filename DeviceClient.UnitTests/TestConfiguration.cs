using Microsoft.Extensions.Configuration;
using System.IO;

namespace DeviceClientTesting.UnitTests
{
    public class TestConfiguration
    {
        private static readonly TestConfiguration instance;

        public string IoTHubConnectionString { get; set; }
        public string IoTHubHost { get; set; }

        public static TestConfiguration Instance => instance;

        static TestConfiguration()
        {
            instance = InitializeConfiguration();
        }

        private static TestConfiguration InitializeConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("testsettings.json", optional: true)
                .AddJsonFile("testsettings.local.json", optional: true);
            var rawConfig = builder.Build();

            var config = new TestConfiguration();
            rawConfig.Bind(config);
            return config;
        }
    }
}
