using NUnit.Framework;
using System.Collections.Generic;
using Microsoft.Azure.Devices.Client;
using Microsoft.Azure.Devices.Client.Transport.Mqtt;
using System.Threading.Tasks;
using System;
using System.Threading;
using System.Diagnostics;
using System.Text;

using hub = Microsoft.Azure.Devices;
using Microsoft.Azure.Devices.Client.Exceptions;

namespace DeviceClientTesting.UnitTests
{
    public class TimeoutTests
    {
        private static int deviceIdSuffix;
        private hub.RegistryManager registryManager;
        private List<hub.Device> activeDevices = new List<hub.Device>();
        private SemaphoreSlim activeDeviceSync = new SemaphoreSlim(1, 1);

        [Test]
        [Timeout(260_000)] // default OperationTimeout on DeviceClient is 240s
        [TestCaseSource(typeof(TimeoutTests), nameof(TestCasesParameters))]
        public async Task ReceiveAsync_Should_Timeout_When_No_Messages_Are_Ready(
                                                                            TransportType transportType,
                                                                            int receiveAsyncTimeoutInMs,
                                                                            int operationsTimeoutInMilliseconds,
                                                                            bool useToken,
                                                                            bool forceCleanupIfNoTimeout)
        {
            
            var device = await GetDevice();
            double receiveAsyncCompletedAfterMs = 0;

            try
            {
                bool forcedTimeout = false;
                var deviceClient = SetupDeviceClient(transportType, operationsTimeoutInMilliseconds, device);

                await deviceClient.OpenAsync(); // ensure it is already opened to have that time not messing with the timeout

                var timeoutTestTask = Task.Run(async () =>
                {
                    using (var cts = new CancellationTokenSource(receiveAsyncTimeoutInMs))
                    {
                        var sw = Stopwatch.StartNew();
                        hub.Client.Message receivedMessage = null;

                        try
                        {
                            if (useToken)
                            {
                                cts.Token.Register(() =>
                                {
                                    Console.WriteLine($"Token canceled: {cts.IsCancellationRequested}");
                                });

                                Console.WriteLine("use token");
                                receivedMessage = await deviceClient.ReceiveAsync(cts.Token);
                            }
                            else
                            {
                                Console.WriteLine($"use timeout: {receiveAsyncTimeoutInMs}");
                                receivedMessage = await deviceClient.ReceiveAsync(TimeSpan.FromMilliseconds(receiveAsyncTimeoutInMs));
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            Console.WriteLine($"Operation canceled after {sw.Elapsed.TotalMilliseconds}");
                        }
                        catch (IotHubCommunicationException ex)
                        {
                            Console.WriteLine($"{nameof(IotHubCommunicationException)} after {sw.Elapsed.TotalMilliseconds}, {ex.Message}");
                        }
                        finally
                        {
                            sw.Stop();
                            receiveAsyncCompletedAfterMs = sw.Elapsed.TotalMilliseconds;
                        }

                        if (!forcedTimeout)
                        {
                            Console.WriteLine($"{nameof(deviceClient.ReceiveAsync)} returned after {sw.Elapsed.TotalSeconds}sec");

                            if (receivedMessage == null)
                            {
                                Console.WriteLine($"\t timed out: {transportType}, {operationsTimeoutInMilliseconds}, {useToken}, {receiveAsyncTimeoutInMs}");
                            }
                            else
                            {
                                var messageData = Encoding.ASCII.GetString(receivedMessage.GetBytes());
                                Console.WriteLine("\t{0}> Received message: {1}", DateTime.Now.ToLocalTime(), messageData);
                                await deviceClient.CompleteAsync(receivedMessage);
                            }
                        }
                    }

                    Console.WriteLine("device client done.");
                });

                if (forceCleanupIfNoTimeout)
                {
                    var result = await Task.WhenAny(timeoutTestTask, Task.Delay(receiveAsyncTimeoutInMs + 2_000));

                    Assert.AreEqual(timeoutTestTask, result, "Did not timeout");

                    if (result == timeoutTestTask)
                    {
                        var diff = receiveAsyncCompletedAfterMs - receiveAsyncTimeoutInMs;
                        Assert.IsTrue(diff < 1_000, $"Timeout occured, but too late: {diff}");
                    }
                    else
                    {
                        forcedTimeout = true;
                        Console.WriteLine("did not timeout. Force cleanup");
                    }
                }
                else
                {
                    await timeoutTestTask;
                    var diff = receiveAsyncCompletedAfterMs - receiveAsyncTimeoutInMs;
                    Assert.IsTrue(diff < 1_000, $"Timeout occured, but too late: {diff}");
                }

                deviceClient.Dispose();
            }
            finally
            {
                await RemoveDevice(device);
            }
        }

        private static DeviceClient SetupDeviceClient(TransportType transportType, int operationsTimeoutInMilliseconds, hub.Device device)
        {
            ITransportSettings tp = null;
            switch (transportType)
            {
                case TransportType.Amqp_Tcp_Only:
                case TransportType.Amqp_WebSocket_Only:
                case TransportType.Amqp:
                    var amqp = new AmqpTransportSettings(transportType);
                    if (operationsTimeoutInMilliseconds > 0)
                    {
                        amqp.OpenTimeout = amqp.OperationTimeout = TimeSpan.FromMilliseconds(operationsTimeoutInMilliseconds);
                    }
                    Console.WriteLine($"AMQP settings: {nameof(amqp.OperationTimeout)}: {amqp.OperationTimeout}, {nameof(amqp.OpenTimeout)}: {amqp.OpenTimeout}");
                    tp = amqp;
                    break;
                case TransportType.Mqtt_Tcp_Only:
                case TransportType.Mqtt_WebSocket_Only:
                case TransportType.Mqtt:
                    var mqtt = new MqttTransportSettings(transportType);
                    if (operationsTimeoutInMilliseconds > 0)
                    {
                        mqtt.DefaultReceiveTimeout = TimeSpan.FromMilliseconds(operationsTimeoutInMilliseconds);
                    }
                    Console.WriteLine($"MQTT settings: {nameof(mqtt.DefaultReceiveTimeout)}: {mqtt.DefaultReceiveTimeout}");
                    tp = mqtt;
                    break;
            }

            var deviceClient = DeviceClient.Create(TestConfiguration.Instance.IoTHubHost,
                                        new DeviceAuthenticationWithRegistrySymmetricKey(device.Id, device.Authentication.SymmetricKey.PrimaryKey),
                                        new ITransportSettings[] { tp });

            if (operationsTimeoutInMilliseconds > 0)
            {
                deviceClient.OperationTimeoutInMilliseconds = (uint)operationsTimeoutInMilliseconds;
            }

            Console.WriteLine($"Created device client with operations timeout = {deviceClient.OperationTimeoutInMilliseconds}");

            return deviceClient;
        }

        private static IEnumerable<object[]> TestCasesParameters
        {
            get {
                // 1. AMQP:
                // CancellationToken: yes (5s timeout)
                // operations timeout: default
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, -1, true, true};
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, -1, true, true};

                // 2. MQTT:
                // CancellationToken: yes (5s timeout)
                // operations timeout: default
                yield return new object[] { TransportType.Mqtt_Tcp_Only, 5_000, -1, true, true};
                yield return new object[] { TransportType.Mqtt_WebSocket_Only, 5_000, -1, true, true};

                // 3. AMQP:
                // CancellationToken: no
                // Timeout: (5s timeout)
                // operations timeout: default
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, -1, false, true};
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, -1, false, true};

                // 4. MQTT:
                // CancellationToken: no
                // Timeout: (5s timeout)
                // operations timeout: default
                yield return new object[] { TransportType.Mqtt_Tcp_Only, 5_000, -1, false, true};
                yield return new object[] { TransportType.Mqtt_WebSocket_Only, 5_000, -1, false, true};

                // 5. AMQP:
                // CancellationToken: yes (5s timeout)
                // operations timeout: 2s
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, 2_000, true, true};
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, 2_000, true, true};
                
                // 6. MQTT:
                // CancellationToken: yes (5s timeout)
                // operations timeout: 2s
                yield return new object[] { TransportType.Mqtt_Tcp_Only, 5_000, 2_000, true, true };
                yield return new object[] { TransportType.Mqtt_WebSocket_Only, 5_000, 2_000, true, true };

                // 7. AMQP:
                // CancellationToken: no
                // Timeout: (5s default)
                // operations timeout: 2s
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, 2_000, false, true };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, 2_000, false, true };

                // 8. MQTT:
                // CancellationToken: no
                // Timeout: (5s default)
                // operations timeout: 2s
                yield return new object[] { TransportType.Mqtt_Tcp_Only, 5_000, 2_000, false, true };
                yield return new object[] { TransportType.Mqtt_WebSocket_Only, 5_000, 2_000, false, true };

                // 9. AMQP
                // CancellationToken: no
                // Timeout: (5s default)
                // operations timeout: default
                // forced cleanup: false
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, 4_000, false, false };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, 4_000, false, false };

                // 10. AMQP
                // CancellationToken: yes (5s default)
                // Timeout: no
                // operations timeout: default
                // forced cleanup: false
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, -1, true, false };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, -1, true, false };

                // 11. AMQP - set operations timeout to the same as the receive timeout: 5s
                // CancellationToken: yes (5s default)
                // Timeout: no
                // operations timeout: 5s
                // forced cleanup: false
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, 5_000, true, false };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, 5_000, true, false };

                // 11. AMQP - set operations timeout to the less as the receive timeout: 4s
                // CancellationToken: yes (5s default)
                // Timeout: no
                // operations timeout: 4s
                // forced cleanup: false
                yield return new object[] { TransportType.Amqp_Tcp_Only, 5_000, 4_000, true, false };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 5_000, 4_000, true, false };

                // 12. AMQP - short receive timeout
                // CancellationToken: no
                // Timeout: 50ms
                // operations timeout: default (60s)
                // forced cleanup: true
                yield return new object[] { TransportType.Amqp_Tcp_Only, 50, -1, false, true };
                yield return new object[] { TransportType.Amqp_WebSocket_Only, 50, -1, false, true };
            }
        }

        [OneTimeSetUp]
        public async Task OneTimeSetup()
        { 
            registryManager = hub.RegistryManager.CreateFromConnectionString(TestConfiguration.Instance.IoTHubConnectionString);
            await registryManager.OpenAsync();
        }

        [OneTimeTearDown]
        public async Task OneTimeTearDown()
        {
            foreach (var device in activeDevices)
            {
                await RemoveDevice(device);
            }


            await registryManager.CloseAsync();
            registryManager.Dispose();
        }

        private async Task<hub.Device> GetDevice()
        {
            var id = $"TimeoutTest{deviceIdSuffix++}";
            var device = new hub.Device(id);
            var activeDevice = await registryManager.GetDeviceAsync(id);
            if (activeDevice == null)
            {
                activeDevice = await this.registryManager.AddDeviceAsync(device);
            }

            await activeDeviceSync.WaitAsync();
            try
            {
                activeDevices.Add(activeDevice);
            }
            finally
            {
                activeDeviceSync.Release();
            }

            return activeDevice;
        }

        private async Task RemoveDevice(hub.Device device)
        {
            await registryManager.RemoveDeviceAsync(device);

            await activeDeviceSync.WaitAsync();
            try
            {
                activeDevices.Remove(device);
            }
            finally
            {
                activeDeviceSync.Release();
            }
        }
    }
}