using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using InfluxData.Net.InfluxDb.Enums;
using InfluxData.Net.InfluxDb.Models;
using InfluxMigrate.Filters;
using InfluxMigrate.Helpers;
using InfluxMigrate.Models;
using InfluxMigrate.Providers;
using InfluxMigrate.Statics;

namespace InfluxMigrate
{
    public class Program
    {
        private static InfluxDbProviderV8 _v8 = new InfluxDbProviderV8();
        private static InfluxDbProviderV9 _v9 = new InfluxDbProviderV9();
        private static readonly IDictionary<string, int> _sensors = SensorsToMigrate.FromV8ToV9Test;
        private static DateTime _fromDate = new DateTime(2015, 9, 1, 0, 0, 0);
        private static DateTime _toDate = DateTime.UtcNow;
        private static int _timeBetweenRequests = 250; // milliseconds (lets the CPU's breathe a bit)

        static void Main(string[] args)
        {
            Task.Run(async () =>
            {
                await RunMigration();
                await RunBackfill();
                //await EnsureAllFieldsExist();
            }).Wait();
        }

        private static async Task RunMigration()
        {
            foreach (var sensor in _sensors)
            {
                var fromDate = _fromDate;

                while (fromDate < _toDate)
                {
                    try
                    {
                        Console.WriteLine(String.Format("\n\n--- {0} - {1} - {2} -------", fromDate, DateTime.Now, sensor));

                        var toDate = fromDate.AddDays(1);
                        var filter = CreateFilter(sensor.Key, fromDate, toDate);
                        var v8readings = await _v8.GetReadings(filter);
                        var v9readings = ConvertToV9(v8readings);


                        if (v9readings.Count > 0)
                        {
                            var response = await _v9.SaveReadings(v9readings);
                            if (response.Success == false)
                            {
                                Console.WriteLine("{0} save failed with {1}!", sensor, response.StatusCode);
                                Console.WriteLine(response.Body);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("{0} exception at {1}", sensor, DateTime.Now);
                        Console.WriteLine(e);
                        Console.WriteLine(e.Message);
                        if (e.InnerException != null)
                            Console.WriteLine(e.InnerException.Message);
                    }

                    fromDate = fromDate.AddDays(1);
                    await Task.Delay(_timeBetweenRequests);
                }

                Console.WriteLine(String.Format("\n\n{0} done at {1}", sensor, DateTime.Now));
            }
        }

        private static async Task RunBackfill()
        {
            foreach (var sensor in _sensors)
            {
                var fromDate = _fromDate;

                while (fromDate < _toDate)
                {
                    var toDate = fromDate.AddDays(7);
                    Console.WriteLine(String.Format("\n\n--- {0} - {1} - {2} -------", fromDate, DateTime.Now, sensor));

                    foreach (var interval in DownsampleIntervalConstants.AllIntervals)
                    {
                        try
                        {
                            var backfill = CreateBackfill(sensor.Key, interval, fromDate, toDate);

                            var response = await _v9.Backfill(backfill);
                            if (response.Success == false)
                            {
                                Console.WriteLine("{0} backfill failed with {1}!", sensor, response.StatusCode);
                                Console.WriteLine(response.Body);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("{0} exception at {1}", sensor, DateTime.Now);
                            Console.WriteLine(e);
                            Console.WriteLine(e.Message);
                            if (e.InnerException != null)
                                Console.WriteLine(e.InnerException.Message);
                        }

                        await Task.Delay(_timeBetweenRequests);
                    }

                    fromDate = fromDate.AddDays(7);
                }

                // The "week" backfill needs to be run separately for better precision (to ensure that the data that's being worked on 
                // does not start in the middle of the week and end in the middle of the week after the starting one)
                //try
                //{
                //    Console.WriteLine(String.Format("\n\n--- {0} - {1} - {2} ONE WEEK BACKFILL -------", _fromDate, DateTime.Now, sensor));
                //    var backfill = CreateBackfill(sensor.Key, DownsampleIntervalConstants.OneWeek, _fromDate, _toDate);

                //    var response = await _v9.Backfill(backfill);
                //    if (response.Success == false)
                //    {
                //        Console.WriteLine("{0} backfill failed with {1}!", sensor, response.StatusCode);
                //        Console.WriteLine(response.Body);
                //    }

                //    await Task.Delay(_timeBetweenRequests);
                //}
                //catch (Exception e)
                //{
                //    Console.WriteLine("{0} exception at {1}", sensor, DateTime.Now);
                //    Console.WriteLine(e);
                //    Console.WriteLine(e.Message);
                //    if (e.InnerException != null)
                //        Console.WriteLine(e.InnerException.Message);
                //}

                Console.WriteLine(String.Format("\n\n{0} done at {1}", sensor, DateTime.Now));
            }
        }

        private static async Task EnsureAllFieldsExist()
        {
            foreach (var sensor in _sensors)
            {
                var fakeReading = new ReadingMessageDTO()
                {
                    SerialNumber = sensor.Key,
                    SensorId = -1,
                    RawMessage = "fakeMessage",
                    Temperature = 0,
                    Humidity = 0,
                    Resistance = 0,
                    DateCreated = DateTime.UtcNow
                };

                await _v8.SaveReading(fakeReading);
                await Task.Delay(_timeBetweenRequests);
            }
        }

        private static ReadingsFetchFilter CreateFilter(string sensorSerial, DateTime timeFrom, DateTime timeTo)
        {
            return new ReadingsFetchFilter()
            {
                SensorSerials = new[] { sensorSerial },
                TimeFrom = timeFrom.ToString("yyyy-MM-dd HH:mm:ss"),
                TimeTo = timeTo.ToString("yyyy-MM-dd HH:mm:ss"),
                Metric = "*"
            };
        }

        private static BackfillParams CreateBackfill(string sensorSerial, string interval, DateTime timeFrom, DateTime timeTo)
        {
            Console.WriteLine("Interval: {0}", interval);

            return new BackfillParams()
            {
                Downsamplers = new List<string>()
                {
                    "mean(Temperature) as Temperature",
                    "mean(Humidity) as Humidity",
                    "mean(Resistance) as Resistance",
                    "min(Temperature) as TemperatureMin",
                    "min(Humidity) as HumidityMin",
                    "min(Resistance) as ResistanceMin",
                    "max(Temperature) as TemperatureMax",
                    "max(Humidity) as HumidityMax",
                    "max(Resistance) as ResistanceMax",
                },
                DsSerieName = String.Format("reading.downsample.{0}", interval),
                SourceSerieName = "reading",
                TimeFrom = timeFrom,
                TimeTo = timeTo,
                Filters = new List<string>()
                {
                    String.Format("SensorSerialCode='{0}'", sensorSerial)
                },
                Interval = interval,
                Tags = new List<string>()
                {
                    InfluxSerieConstants.SensorId,
                    InfluxSerieConstants.SerialNumber
                },
                FillType = FillType.None
            };
        }

        private static IList<ReadingMessageDTO> ConvertToV9(IList<InfluxDB.Net.Models.Serie> series)
        {
            var readings = new List<ReadingMessageDTO>();

            if (series.Count == 0)
                Console.WriteLine("No data");

            foreach (var serie in series)
            {
                Console.WriteLine("Points: {0}", serie.Points.Count());

                var sensorIdIndex = Array.IndexOf(serie.Columns, "SensorId");
                var rawMessageIndex = Array.IndexOf(serie.Columns, "RawMessage");
                var temperatureIndex = Array.IndexOf(serie.Columns, "Temperature");
                var humidityIndex = Array.IndexOf(serie.Columns, "Humidity");
                var resistanceIndex = Array.IndexOf(serie.Columns, "Resistance");
                var timeIndex = Array.IndexOf(serie.Columns, "time");


                foreach (var point in serie.Points)
                {
                    var sensorId = point[sensorIdIndex];
                    var rawMessage = point[rawMessageIndex];
                    var temperature = point[temperatureIndex];
                    var humidity = point[humidityIndex];
                    var resistance = point[resistanceIndex];
                    var timestamp = point[timeIndex];

                    var reading = new ReadingMessageDTO()
                    {
                        SerialNumber = serie.Name,
                        SensorId = SensorsToMigrate.FromV8ToV9[serie.Name],
                        RawMessage = rawMessage.ToString(),
                        Temperature = Int32.Parse(temperature.ToString()),
                        Humidity = Int32.Parse(humidity.ToString()),
                        Resistance = Int32.Parse(resistance.ToString()),
                        DateCreated = TimeHelper.UnixTimestampToDateTime(Int64.Parse(timestamp.ToString()))
                    };

                    readings.Add(reading);
                }
            }

            return readings;
        }
    }
}
